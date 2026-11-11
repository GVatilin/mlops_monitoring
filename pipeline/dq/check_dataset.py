import logging
from datetime import datetime, timezone

import pandas as pd

from pipeline.settings import CURRENT_DATASET, LABEL

logger = logging.getLogger(__name__)

MIN_FRAUD_RATE = 0.001
MAX_FRAUD_RATE = 0.02


def check_dataset() -> dict:
    """Проверяет current.parquet по правилам DQ из README. """
    frame = pd.read_parquet(CURRENT_DATASET)
    months = sorted(pd.to_datetime(frame["transaction_time"]).dt.to_period("M").unique())
    nulls = int(frame.isna().any(axis=1).sum())
    duplicates = int(frame.duplicated().sum())
    bad_target = int((~frame[LABEL].isin([0, 1])).sum())
    bad_amount = int((~(frame["amount"] > 0)).sum())
    bad_coords = int(
        (
            ~frame["lat"].between(-90, 90)
            | ~frame["merchant_lat"].between(-90, 90)
            | ~frame["lon"].between(-180, 180)
            | ~frame["merchant_lon"].between(-180, 180)
        ).sum()
    )
    consecutive = len(months) == 3 and (months[-1] - months[0]).n == 2
    fraud_rate = float(frame[LABEL].mean())

    rules = [
        ("Полнота", "Ни в одной колонке нет пропусков",
         nulls == 0, f"Строк с пропусками: {nulls}"),
        ("Уникальность", "Нет полностью одинаковых строк",
         duplicates == 0, f"Дублей: {duplicates}"),
        ("Метка", "target принимает только значения 0 и 1",
         bad_target == 0, f"Строк с другой меткой: {bad_target}"),
        ("Сумма", "amount больше нуля",
         bad_amount == 0, f"Строк с amount ≤ 0 или без суммы: {bad_amount}"),
        ("Координаты", "Широта в [-90, 90], долгота в [-180, 180] у клиента и мерчанта",
         bad_coords == 0, f"Строк вне диапазона: {bad_coords}"),
        ("Окно", "Ровно три подряд идущих календарных месяца",
         consecutive, "Месяцы: " + ", ".join(str(month) for month in months)),
        ("Доля фрода", f"Доля target = 1 от {MIN_FRAUD_RATE:.1%} до {MAX_FRAUD_RATE:.0%}",
         MIN_FRAUD_RATE <= fraud_rate <= MAX_FRAUD_RATE, f"Доля фрода: {fraud_rate:.2%}"),
    ]
    results = [
        {"name": name, "description": description, "passed": bool(passed), "details": details}
        for name, description, passed, details in rules
    ]
    for result in results:
        logger.info("%s: %s. %s", result["name"], "OK" if result["passed"] else "FAIL", result["details"])
    return {
        "dataset": str(CURRENT_DATASET),
        "rows": int(len(frame)),
        "window": f"{months[0]}_{months[-1]}" if months else "empty",
        "checked_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
        "rules": results,
    }
