"""Расчёт задержек повторов — общий для outbox-воркера и консьюмеров."""


def backoff_seconds(attempts: int, base: float, cap: float) -> float:
    """Экспоненциальная задержка с ограничением сверху.

    Формула: ``base * 2 ** (attempts - 1)``, нумерация попыток с единицы.
    Для ``attempts <= 0`` возвращается ``base``.
    """
    if attempts <= 0:
        return base
    return float(min(cap, base * (2 ** (attempts - 1))))
