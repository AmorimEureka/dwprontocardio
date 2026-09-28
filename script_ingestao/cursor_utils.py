from decimal import Decimal, InvalidOperation


def normalize_numeric_cursor(value, initial_value):
    """Normaliza cursores numéricos vindos do Oracle ou do estado do dlt."""
    if value is None:
        return None

    try:
        parsed = Decimal(str(value).strip())
    except (InvalidOperation, TypeError, ValueError):
        return None

    if isinstance(initial_value, bool):
        return value
    if isinstance(initial_value, int):
        if parsed != parsed.to_integral_value():
            return None
        return int(parsed)
    if isinstance(initial_value, float):
        return float(parsed)
    if isinstance(initial_value, Decimal):
        return parsed

    return value
