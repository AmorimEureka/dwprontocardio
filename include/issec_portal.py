from __future__ import annotations

import json
import re
import unicodedata
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from html.parser import HTMLParser
from urllib.parse import urljoin

import requests
import dlt
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


DEFAULT_BASE_URL = "http://144.22.149.116:8080/ords/"
LOGIN_PATH = "r/aplicacoes/portal-credenciado/login"
CONSULTA_PATH = "r/aplicacoes/portal-credenciado/consulta-de-processos"
REGION_ID = "462759735700133739"
PAGE_SIZE = 50


class IssecPortalError(RuntimeError):
    pass


@dataclass
class HtmlPage:
    values_by_id: dict[str, str] = field(default_factory=dict)
    links: list[str] = field(default_factory=list)
    rows: list[list[dict]] = field(default_factory=list)


class _PortalHtmlParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.page = HtmlPage()
        self._row: list[dict] | None = None
        self._cell: dict | None = None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]):
        data = dict(attrs)
        if tag == "input" and data.get("id"):
            self.page.values_by_id[data["id"]] = data.get("value") or ""
        if tag == "a" and data.get("href"):
            href = data["href"] or ""
            self.page.links.append(href)
            if self._cell is not None:
                self._cell["links"].append(href)
        if tag == "tr":
            self._row = []
        if tag in {"td", "th"} and self._row is not None:
            self._cell = {"text": "", "links": []}

    def handle_endtag(self, tag: str):
        if tag in {"td", "th"} and self._row is not None and self._cell:
            self._cell["text"] = " ".join(self._cell["text"].split())
            self._row.append(self._cell)
            self._cell = None
        if tag == "tr" and self._row is not None:
            if self._row:
                self.page.rows.append(self._row)
            self._row = None

    def handle_data(self, data: str):
        if self._cell is not None:
            self._cell["text"] += data


def parse_html(html: str) -> HtmlPage:
    parser = _PortalHtmlParser()
    parser.feed(html)
    return parser.page


def normalize_label(value: str) -> str:
    value = unicodedata.normalize("NFKD", value)
    value = "".join(char for char in value if not unicodedata.combining(char))
    return re.sub(r"[^a-z0-9]+", " ", value.casefold()).strip()


def parse_brl(value: str) -> Decimal:
    cleaned = re.sub(r"[^0-9,.-]", "", str(value or ""))
    if not cleaned:
        return Decimal("0.00")
    return Decimal(cleaned.replace(".", "").replace(",", ".")).quantize(
        Decimal("0.01")
    )


def extract_dialog_path(script: str, page_name: str) -> str:
    match = re.search(rf"dialog\('([^']*{re.escape(page_name)}[^']*)'", script)
    if not match:
        raise IssecPortalError(f"Redirecionamento {page_name!r} não encontrado.")
    return match.group(1).replace("\\u002F", "/").replace("\\u0026", "&")


def parse_process_links(html: str) -> list[tuple[str, str]]:
    links = []
    for href in parse_html(html).links:
        if "relnumproc" not in href:
            continue
        path = extract_dialog_path(href, "relnumproc")
        process_match = re.search(r"[?&]nu_proc=([^&]+)", path)
        if process_match:
            links.append((process_match.group(1).strip(), path))
    return links


def parse_process_detail(html: str, expected_process: str) -> dict:
    rows = parse_html(html).rows
    values: dict[str, str] = {}
    for index in range(len(rows) - 1):
        headers = [normalize_label(cell["text"]) for cell in rows[index]]
        next_values = [cell["text"] for cell in rows[index + 1]]
        if not headers or len(headers) != len(next_values):
            continue
        for label, value in zip(headers, next_values, strict=True):
            values[label] = value
    processo = values.get("processo", expected_process).strip()
    if processo != expected_process:
        raise IssecPortalError(
            f"Detalhe retornou processo {processo!r}; esperado {expected_process!r}."
        )
    try:
        mes_producao = datetime.strptime(
            values["mes de producao"], "%d/%m/%Y"
        ).date()
    except (KeyError, ValueError) as exc:
        raise IssecPortalError(
            f"Mês de produção ausente ou inválido no processo {processo}."
        ) from exc
    return {
        "processo": processo,
        "mes_producao": mes_producao,
        "valor_cobrado": parse_brl(values.get("valor cobrado r", "")),
        "valor_liberado": parse_brl(values.get("valor liberado r", "")),
        "valor_servico": parse_brl(values.get("valor servico r", "")),
        "valor_total_itens_glosados": parse_brl(
            values.get("valor total de itens glosados", "")
        ),
    }


def month_range(start: date, end: date) -> list[date]:
    current = start.replace(day=1)
    end = end.replace(day=1)
    result = []
    while current <= end:
        result.append(current)
        current = date(
            current.year + (current.month == 12),
            1 if current.month == 12 else current.month + 1,
            1,
        )
    return result


def select_months_to_scrape(
    today: date,
    extracted_months: set[date],
    start: date = date(2026, 1, 1),
) -> list[date]:
    available = month_range(start, today)
    refresh = set(available[-3:])
    return [month for month in available if month not in extracted_months or month in refresh]


class IssecPortalClient:
    def __init__(
        self,
        username: str,
        password: str,
        *,
        base_url: str = DEFAULT_BASE_URL,
        timeout: int = 30,
        max_workers: int = 4,
    ) -> None:
        if not username or not password:
            raise IssecPortalError("Credenciais do portal ISSEC não configuradas.")
        self.username = username
        self.password = password
        self.base_url = base_url.rstrip("/") + "/"
        self.timeout = timeout
        self.max_workers = max_workers
        self.session = self._new_session()
        self.session_id = ""

    def _new_session(self) -> requests.Session:
        session = requests.Session()
        retry = Retry(
            total=3,
            connect=3,
            read=3,
            backoff_factor=1,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=("GET", "POST"),
        )
        session.mount("http://", HTTPAdapter(max_retries=retry))
        session.mount("https://", HTTPAdapter(max_retries=retry))
        session.headers["User-Agent"] = "ReceitaCerta-ISSEC/1.0"
        return session

    def _get(self, path: str) -> requests.Response:
        response = self.session.get(
            urljoin(self.base_url, path), timeout=self.timeout
        )
        response.raise_for_status()
        return response

    def _submit(
        self,
        page: HtmlPage,
        request: str,
        items: list[tuple[str, str]],
        referer: str,
    ) -> requests.Response:
        required = {
            "pFlowId",
            "pFlowStepId",
            "pInstance",
            "pPageSubmissionId",
            "pReloadOnSubmit",
            "pContext",
            "pSalt",
            "pPageItemsProtected",
        }
        missing = required - page.values_by_id.keys()
        if missing:
            raise IssecPortalError(
                "Campos APEX ausentes: " + ", ".join(sorted(missing))
            )
        payload_json = {
            "pageItems": {
                "itemsToSubmit": [
                    {"n": name, "v": value} for name, value in items
                ],
                "protected": page.values_by_id["pPageItemsProtected"],
                "rowVersion": page.values_by_id.get(
                    "pPageItemsRowVersion", ""
                ),
                "formRegionChecksums": json.loads(
                    page.values_by_id.get("pPageFormRegionChecksums", "[]")
                ),
            },
            "salt": page.values_by_id["pSalt"],
        }
        data = {
            "p_flow_id": page.values_by_id["pFlowId"],
            "p_flow_step_id": page.values_by_id["pFlowStepId"],
            "p_instance": page.values_by_id["pInstance"],
            "p_debug": "",
            "p_request": request,
            "p_reload_on_submit": page.values_by_id["pReloadOnSubmit"],
            "p_page_submission_id": page.values_by_id["pPageSubmissionId"],
            "p_json": json.dumps(payload_json, separators=(",", ":")),
        }
        response = self.session.post(
            urljoin(
                self.base_url,
                f"wwv_flow.accept?p_context={page.values_by_id['pContext']}",
            ),
            data=data,
            headers={"Referer": referer, "X-Requested-With": "XMLHttpRequest"},
            timeout=self.timeout,
        )
        response.raise_for_status()
        return response

    def login(self) -> None:
        login_url = urljoin(self.base_url, LOGIN_PATH)
        login_page = parse_html(self._get(LOGIN_PATH).text)
        response = self._submit(
            login_page,
            "LOGIN",
            [
                ("P9999_USERNAME", self.username),
                ("P9999_PASSWORD", self.password),
                ("P9999_REMEMBER", "N"),
            ],
            login_url,
        )
        redirect = response.json().get("redirectURL", "")
        if "notification_msg" in redirect or "/home" not in redirect:
            raise IssecPortalError("Autenticação recusada pelo portal ISSEC.")
        home = self._get(redirect)
        self.session_id = parse_html(home.text).values_by_id.get(
            "pInstance", ""
        )
        if not self.session_id:
            raise IssecPortalError("Sessão APEX não foi criada.")

    def _report_page(self, month: date) -> tuple[str, HtmlPage, str]:
        if not self.session_id:
            self.login()
        consulta_path = f"{CONSULTA_PATH}?session={self.session_id}"
        consulta_url = urljoin(self.base_url, consulta_path)
        page = parse_html(self._get(consulta_path).text)
        response = self._submit(
            page,
            "bt-PesqPeriodo",
            [
                ("P19_NU_PROC", ""),
                ("P19_PROCMES", f"{month.month:02d}"),
                ("P19_PROCANO", str(month.year)),
            ],
            consulta_url,
        )
        redirect = response.json().get("redirectURL", "")
        report_path = extract_dialog_path(redirect, "relprocperiodo")
        report_response = self._get(report_path)
        return report_path, parse_html(report_response.text), report_response.text

    def _report_ajax(
        self,
        report_path: str,
        report_page: HtmlPage,
        report_html: str,
        *,
        first_row: int | None = None,
    ) -> str:
        match = re.search(
            rf'apex\.widget\.report\.init\("R{REGION_ID}","([^"]+)"',
            report_html,
        )
        if not match:
            raise IssecPortalError("Identificador AJAX do relatório não encontrado.")
        ajax_identifier = match.group(1).replace("\\u002F", "/")
        data = {
            "p_flow_id": report_page.values_by_id["pFlowId"],
            "p_flow_step_id": report_page.values_by_id["pFlowStepId"],
            "p_instance": report_page.values_by_id["pInstance"],
            "p_debug": "",
            "p_request": f"PLUGIN={ajax_identifier}",
            "p_widget_action": "paginate" if first_row else "reset",
            "x01": REGION_ID,
            "p_json": json.dumps(
                {"salt": report_page.values_by_id["pSalt"]},
                separators=(",", ":"),
            ),
        }
        if first_row:
            data.update(
                {
                    "p_pg_min_row": str(first_row),
                    "p_pg_max_rows": str(PAGE_SIZE),
                    "p_pg_rows_fetched": str(PAGE_SIZE),
                }
            )
        context = f"portal-credenciado/relprocperiodo/{self.session_id}"
        response = self.session.post(
            urljoin(self.base_url, f"wwv_flow.ajax?p_context={context}"),
            data=data,
            headers={
                "Referer": urljoin(self.base_url, report_path),
                "X-Requested-With": "XMLHttpRequest",
            },
            timeout=self.timeout,
        )
        response.raise_for_status()
        return response.text

    def list_processes(self, month: date) -> list[tuple[str, str]]:
        report_path, report_page, report_html = self._report_page(month)
        first_row = 1
        found: dict[str, str] = {}
        while True:
            html = self._report_ajax(
                report_path,
                report_page,
                report_html,
                first_row=None if first_row == 1 else first_row,
            )
            links = parse_process_links(html)
            new_count = 0
            for process, path in links:
                if process not in found:
                    found[process] = path
                    new_count += 1
            if len(links) < PAGE_SIZE or new_count == 0:
                break
            first_row += PAGE_SIZE
        return list(found.items())

    def _detail(self, item: tuple[str, str]) -> dict:
        process, path = item
        session = self._new_session()
        session.cookies.update(self.session.cookies)
        response = session.get(urljoin(self.base_url, path), timeout=self.timeout)
        response.raise_for_status()
        return parse_process_detail(response.text, process)

    def scrape_month(self, month: date) -> list[dict]:
        links = self.list_processes(month)
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            rows = list(executor.map(self._detail, links))
        extracted_at = datetime.now().astimezone().isoformat()
        for row in rows:
            row["competencia"] = month.strftime("%m/%Y")
            row["extraido_em"] = extracted_at
        return rows


@dlt.source(name="portal_issec")
def portal_issec_source(
    *,
    username: str,
    password: str,
    months: list[date],
    base_url: str = DEFAULT_BASE_URL,
    timeout: int = 30,
    max_workers: int = 4,
):
    """Fonte dlt que encadeia login, pesquisa, paginação e detalhe APEX."""

    @dlt.resource(
        name="processos",
        primary_key="processo",
        write_disposition="merge",
    )
    def processos():
        client = IssecPortalClient(
            username=username,
            password=password,
            base_url=base_url,
            timeout=timeout,
            max_workers=max_workers,
        )
        client.login()
        for month in months:
            yield from client.scrape_month(month)

    return processos
