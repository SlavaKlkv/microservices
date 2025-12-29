import re


def validate_username_value(v: str, pattern: re.Pattern[str]) -> str:
    normalized = v.casefold().strip()
    if not pattern.fullmatch(normalized):
        raise ValueError(
            'username может содержать только '
            "латинские буквы, цифры, символы '._-'"
        )
    return normalized


def validate_full_name_value(v: str) -> str | None:
    if v is None:
        return v
    if v.strip() == '':
        raise ValueError('full_name не может быть пустой строкой')
    return v.strip()


def validate_password_value(v: str) -> str:
    if v.strip() != v:
        raise ValueError('пароль не должен начинаться/заканчиваться пробелом')
    if not any(c.isalpha() for c in v) or not any(c.isdigit() for c in v):
        raise ValueError('пароль должен содержать буквы и цифры')
    return v
