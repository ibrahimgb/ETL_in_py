import re
from datetime import datetime, timezone
from typing import Callable


class Formater:
    @staticmethod
    def field_parser(*parsers: Callable[[str | None], str | None]) -> Callable[[str | None], str | None]:
        def apply_parsers(value: str | None) -> str | None:
            for parser in parsers:
                value = parser(value)
            return value
        return apply_parsers

    @staticmethod
    def to_datetime(format: str) -> Callable[[str | None], str | None]:
        def parse(value: str | None) -> str | None:
            if not value:
                return None
            return datetime.strptime(value, format).replace(tzinfo=timezone.utc).isoformat()
        return parse

    @staticmethod
    def to_date(format: str) -> Callable[[str | None], str | None]:
        def parse(value: str | None) -> str | None:
            if not value:
                return None
            return datetime.strptime(value, format).replace(tzinfo=timezone.utc).date().isoformat()
        return parse

    @staticmethod
    def to_null(from_value: str) -> Callable[[str | None], str | None]:
        def parse(value: str | None) -> str | None:
            return None if value == from_value else value
        return parse

    @staticmethod
    def to_int(value: str | None) -> int | None:
        if not value:
            return None
        return int(float(value))

    @staticmethod
    def to_float(value: str | None) -> float | None:
        if not value:
            return None
        return float(value)

    @staticmethod
    def null_to_empty_string(value: str | None) -> str:
        return "" if value is None else value

    @staticmethod
    def left_trim(chars: str) -> Callable[[str | None], str | None]:
        def parse(value: str | None) -> str | None:
            if value is None:
                return None
            return value.lstrip(chars)
        return parse

    @staticmethod
    def replace(to_replace: str, replace_by: str) -> Callable[[str | None], str | None]:
        def parse(value: str | None) -> str | None:
            if value is None:
                return None
            return value.replace(to_replace, replace_by)
        return parse

    @staticmethod
    def regex_replace(to_replace: str, replace_by: str) -> Callable[[str | None], str | None]:
        def parse(value: str | None) -> str | None:
            if value is None:
                return None
            return re.sub(to_replace, replace_by, value)
        return parse

    @staticmethod
    def to_upper(value: str | None) -> str | None:
        if value is None:
            return None
        return value.upper()

    @staticmethod
    def null_to_zero(value: str | None) -> int | str | None:
        return 0 if value is None else value

    @staticmethod
    def to_boolean(value: str | None) -> bool | str | None:
        if value is None:
            return None
        value = value.lower()
        if value in ("y", "yes", "t", "true", "on", "1"):
            return True
        elif value in ("n", "no", "f", "false", "off", "0"):
            return False
        return value

    @staticmethod
    def to_int(value: str | None) -> int | None:
        if value is None:
            return None
        try:
            return int(float(value))
        except (ValueError, TypeError):
            raise ValueError(f"Cannot convert value '{value}' to integer.")

    @staticmethod
    def to_float(value: str | None, precision: int = 2) -> float | None:
        if value is None:
            return None
        try:
            number = float(value)
            return round(number, precision)
        except (ValueError, TypeError):
            raise ValueError(f"Cannot convert value '{value}' to float.")
