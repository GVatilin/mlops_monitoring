"""Очистка, препроцессинг и отбор признаков для train и test."""

import logging
import sys

import pandas as pd

from pipeline.settings import FRAUD_SRC, LABEL, SPLITS_DIR

logger = logging.getLogger(__name__)

REQUIRED_COLUMNS = ["transaction_time", "amount", LABEL]
UNSTABLE_FEATURES = ["year", "month", "day", "minute"]


def clean_raw(frame: pd.DataFrame, part: str) -> pd.DataFrame:
    before = len(frame)
    cleaned = frame.dropna(subset=REQUIRED_COLUMNS)
    cleaned = cleaned.loc[cleaned[LABEL].isin([0, 1]) & (cleaned["amount"] > 0)]
    cleaned = cleaned.drop_duplicates().astype({LABEL: int})
    if cleaned.empty:
        raise RuntimeError(f"После очистки в {part} не осталось строк из {before}")
    return cleaned.reset_index(drop=True)


def preprocess_dataset(split_info: dict) -> dict:
    """Чистит сырые train и test, строит признаки и отбирает устойчивые.

    Признаки строит run_preproc из fraud_detector/src, та же функция, что
    в онлайн-скоринге. После неё отбрасываются только UNSTABLE_FEATURES,
    поэтому каждый признак модели есть и в потоке. Колонка target остаётся.
    Пишет splits/train_proc.parquet и splits/test_proc.parquet.
    Возвращает пути к ним и метаданные месяца, нужные следующим задачам.
    """
    if FRAUD_SRC not in sys.path:
        sys.path.insert(0, FRAUD_SRC)
    from preprocessing import run_preproc

    train = clean_raw(pd.read_parquet(split_info["train_raw_path"]), "train")
    test = clean_raw(pd.read_parquet(split_info["test_raw_path"]), "test")
    train = run_preproc(train).drop(columns=UNSTABLE_FEATURES)
    test = run_preproc(test).drop(columns=UNSTABLE_FEATURES)

    train_path = SPLITS_DIR / "train_proc.parquet"
    test_path = SPLITS_DIR / "test_proc.parquet"
    train.to_parquet(train_path, index=False)
    test.to_parquet(test_path, index=False)
    logger.info("После препроцессинга: train %s, test %s", train.shape, test.shape)
    return {
        "train_proc_path": str(train_path),
        "test_proc_path": str(test_path),
        "dataset_path": split_info["dataset_path"],
        "batch_month": split_info["batch_month"],
        "train_months": split_info["train_months"],
        "test_month": split_info["test_month"],
    }
