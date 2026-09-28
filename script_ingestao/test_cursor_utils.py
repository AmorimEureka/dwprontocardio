from decimal import Decimal

from script_ingestao.cursor_utils import normalize_numeric_cursor


def test_normaliza_codigo_oracle_textual_com_zero_a_esquerda():
    assert normalize_numeric_cursor("07", 1) == 7
    assert normalize_numeric_cursor("0", 1) == 0


def test_preserva_tipo_do_watermark_numerico():
    assert normalize_numeric_cursor("10.5", 1.0) == 10.5
    assert normalize_numeric_cursor("10.5", Decimal("1")) == Decimal("10.5")


def test_rejeita_valores_nao_numericos_e_fracionarios_para_cursor_inteiro():
    assert normalize_numeric_cursor("A7", 1) is None
    assert normalize_numeric_cursor("7.5", 1) is None


if __name__ == "__main__":
    test_normaliza_codigo_oracle_textual_com_zero_a_esquerda()
    test_preserva_tipo_do_watermark_numerico()
    test_rejeita_valores_nao_numericos_e_fracionarios_para_cursor_inteiro()
