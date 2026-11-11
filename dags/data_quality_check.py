"""DAG проверки качества данных окна обучения.

Файл только связывает задачи. Логика каждой задачи лежит в pipeline.dq.
"""

from datetime import datetime, timedelta

from airflow.decorators import dag, task

from pipeline.dq.check_dataset import check_dataset
from pipeline.dq.publish_dq_report import publish_dq_report


@dag(
    dag_id="data_quality_check",
    description="Раз в 5 минут проверяет окно обучения по правилам DQ и публикует HTML-отчёт",
    schedule="*/5 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=10),
    default_args={"retries": 0},
    tags=["mlops", "dq"],
    doc_md="""
### Проверка качества данных

Каждый запуск проверяет текущее окно обучения `current.parquet`:

1. `check_dataset` применяет правила DQ из README и считает нарушения.
2. `publish_dq_report` пишет HTML-отчёт в `/opt/airflow/dq_reports`, рядом с каталогом моделей.
   Если нарушено хотя бы одно правило, задача падает уже после записи отчёта.
    """,
)
def data_quality_check():
    """Связывает проверку окна обучения и публикацию отчёта."""
    checked = task(check_dataset, task_id="check_dataset")()
    task(publish_dq_report, task_id="publish_dq_report")(checked)


data_quality_check()
