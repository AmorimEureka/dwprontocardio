from datetime import date
from decimal import Decimal

from include.issec_portal import (
    parse_brl,
    parse_process_detail,
    parse_process_links,
    select_months_to_scrape,
)


def test_parse_brl():
    assert parse_brl("R$1.633,06") == Decimal("1633.06")
    assert parse_brl("R$0,00") == Decimal("0.00")


def test_parse_process_links():
    html = """
    <a href="javascript:apex.theme42.dialog('\\u002Fords\\u002Frelnumproc?nu_proc=2600003585\\u0026session=1',{})"></a>
    """
    assert parse_process_links(html) == [
        ("2600003585", "/ords/relnumproc?nu_proc=2600003585&session=1")
    ]


def test_parse_process_detail():
    html = """
    <table>
      <tr><th>Processo:</th><th>Mês de Produção:</th></tr>
      <tr><td>2600003585</td><td>01/01/2026</td></tr>
      <tr><th>Valor Cobrado(R$):</th><th>Valor Liberado(R$):</th></tr>
      <tr><td>R$1.633,06</td><td>R$1.580,74</td></tr>
      <tr><th>Valor Serviço(R$):</th><th>Valor Total de Ítens Glosados:</th></tr>
      <tr><td>R$1.580,74</td><td>R$52,32</td></tr>
    </table>
    """
    assert parse_process_detail(html, "2600003585") == {
        "processo": "2600003585",
        "mes_producao": date(2026, 1, 1),
        "valor_cobrado": Decimal("1633.06"),
        "valor_liberado": Decimal("1580.74"),
        "valor_servico": Decimal("1580.74"),
        "valor_total_itens_glosados": Decimal("52.32"),
    }


def test_select_months_reprocessa_tres_ultimos_e_inclui_ausentes():
    extracted = {
        date(2026, 1, 1),
        date(2026, 2, 1),
        date(2026, 4, 1),
        date(2026, 5, 1),
        date(2026, 6, 1),
        date(2026, 7, 1),
    }
    assert select_months_to_scrape(date(2026, 10, 8), extracted) == [
        date(2026, 3, 1),
        date(2026, 8, 1),
        date(2026, 9, 1),
        date(2026, 10, 1),
    ]
