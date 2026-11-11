"""Публикация HTML-отчёта о качестве данных."""

import html
import logging
from string import Template

from pipeline.settings import DQ_REPORTS_DIR

logger = logging.getLogger(__name__)

PAGE = Template("""<!doctype html>
<html lang="ru">
<head>
<meta charset="utf-8">
<title>DQ $window</title>
<style>
  body { font-family: sans-serif; margin: 32px; color: #222; }
  table { border-collapse: collapse; }
  th, td { border: 1px solid #ccc; padding: 6px 12px; text-align: left; }
  .ok { background: #e6f4ea; }
  .fail { background: #fce8e6; }
</style>
</head>
<body>
<h1>Проверка качества данных: $status</h1>
<p>Датасет: $dataset<br>Строк: $rows<br>Проверено: $checked_at</p>
<table>
<tr><th>Правило</th><th>Описание</th><th>Результат</th><th>Детали</th></tr>
$rows_html
</table>
</body>
</html>
""")


def publish_dq_report(check: dict) -> str:
    """Пишет HTML-отчёт в DQ_REPORTS_DIR и падает, если нарушено хотя бы одно правило. """
    failed = [rule["name"] for rule in check["rules"] if not rule["passed"]]
    rows_html = "\n".join(
        f"<tr class=\"{'ok' if rule['passed'] else 'fail'}\">"
        f"<td>{html.escape(rule['name'])}</td>"
        f"<td>{html.escape(rule['description'])}</td>"
        f"<td>{'OK' if rule['passed'] else 'FAIL'}</td>"
        f"<td>{html.escape(rule['details'])}</td></tr>"
        for rule in check["rules"]
    )
    page = PAGE.substitute(
        window=check["window"],
        status="нарушены правила: " + ", ".join(failed) if failed else "все правила выполнены",
        dataset=html.escape(check["dataset"]),
        rows=check["rows"],
        checked_at=check["checked_at"],
        rows_html=rows_html,
    )

    DQ_REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    report_path = DQ_REPORTS_DIR / f"dq_{check['window']}.html"
    report_path.write_text(page, encoding="utf-8")
    logger.info("Отчёт DQ сохранён: %s", report_path)
    if failed:
        raise ValueError(f"Нарушены правила DQ: {', '.join(failed)}. Отчёт: {report_path}")
    return str(report_path)
