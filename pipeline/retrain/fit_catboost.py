"""Обучение CatBoost на новом трёхмесячном окне."""

import logging
from datetime import datetime, timezone

import pandas as pd

from pipeline.settings import CAT_FEATURES, LABEL, MODELS_DIR

logger = logging.getLogger(__name__)


def prepare_features(frame: pd.DataFrame) -> tuple[pd.DataFrame, pd.Series]:
    """Отделяет target от признаков и приводит категориальные колонки к строкам.

    Категории совпадают с теми, что ожидает онлайн-скоринг после run_preproc.
    Возвращает матрицу признаков и вектор целевой переменной.
    """
    if LABEL not in frame.columns:
        raise ValueError(f"В датасете нет колонки {LABEL}")
    features = frame.drop(columns=[LABEL]).copy()
    for column in CAT_FEATURES:
        if column not in features.columns:
            raise ValueError(f"После препроцессинга нет категориального признака {column}")
        features[column] = features[column].astype(str)
    return features, frame[LABEL]


def score_champion(x_test: pd.DataFrame, y_test: pd.Series) -> tuple[str, float | None]:
    """Считает PR-AUC модели из latest.txt на том же test-месяце. """
    from catboost import CatBoostClassifier
    from sklearn.metrics import average_precision_score

    latest = MODELS_DIR / "latest.txt"
    version = latest.read_text(encoding="utf-8").strip() if latest.exists() else ""
    if version in ("", "initial"):
        return version, None
    champion = CatBoostClassifier()
    champion.load_model(str(MODELS_DIR / f"my_catboost_{version}.cbm"))
    proba = champion.predict_proba(x_test[champion.feature_names_])[:, 1]
    return version, float(average_precision_score(y_test, proba))


def fit_catboost(preproc_info: dict) -> dict:
    """Обучает CatBoost на train и сравнивает его с моделью из прода на свежем месяце.

    Test-месяц в обучении не участвует, поэтому обе модели видят его впервые.
    Свежая модель идёт в прод, если её PR-AUC на test не ниже, чем у текущей.
    Модель сохраняется во временный .cbm в каталоге моделей, в latest.txt
    не публикуется. Возвращает версию, путь к черновику, метрики и решение.
    """
    from catboost import CatBoostClassifier
    from sklearn.metrics import average_precision_score, f1_score, roc_auc_score

    train = pd.read_parquet(preproc_info["train_proc_path"])
    test = pd.read_parquet(preproc_info["test_proc_path"])
    x_train, y_train = prepare_features(train)
    x_test, y_test = prepare_features(test)

    model = CatBoostClassifier(
        iterations=100,
        depth=6,
        learning_rate=0.1,
        loss_function="Logloss",
        auto_class_weights="Balanced",
        random_seed=42,
        allow_writing_files=False,
        verbose=20,
    )
    model.fit(x_train, y_train, cat_features=CAT_FEATURES)

    proba = model.predict_proba(x_test)[:, 1]
    pr_auc = float(average_precision_score(y_test, proba))
    roc_auc = float(roc_auc_score(y_test, proba))
    f1 = float(f1_score(y_test, (proba > 0.5).astype(int), zero_division=0))
    champion_version, champion_pr_auc = score_champion(x_test, y_test)
    publish = champion_pr_auc is None or pr_auc >= champion_pr_auc
    logger.info(
        "Test %s: PR-AUC свежей %.4f, у %s %s. ROC-AUC=%.4f, F1=%.4f. Публикуем: %s",
        preproc_info["test_month"],
        pr_auc,
        champion_version,
        "без сравнения" if champion_pr_auc is None else f"{champion_pr_auc:.4f}",
        roc_auc,
        f1,
        publish,
    )

    version = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    MODELS_DIR.mkdir(parents=True, exist_ok=True)
    staging_model = MODELS_DIR / f".staging_{version}.cbm"
    model.save_model(str(staging_model))
    logger.info("Черновик модели сохранён: %s", staging_model)
    return {
        "version": version,
        "staging_model": str(staging_model),
        "dataset_path": preproc_info["dataset_path"],
        "batch_month": preproc_info["batch_month"],
        "train_months": preproc_info["train_months"],
        "test_month": preproc_info["test_month"],
        "train_rows": int(len(x_train)),
        "test_rows": int(len(x_test)),
        "pr_auc": pr_auc,
        "roc_auc": roc_auc,
        "f1": f1,
        "champion_version": champion_version,
        "champion_pr_auc": champion_pr_auc,
        "publish": publish,
    }
