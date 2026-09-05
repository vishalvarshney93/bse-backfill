"""Compatibility adapter for BSE announcement ranges wider than one year.

The pinned FilingForge engine sends one request spanning ``years``. BSE returns
no Table for wide ranges, so production backfills split announcements into
one-year windows while retaining FilingForge's classification and annual-report
archive behavior.
"""

from __future__ import annotations

from datetime import date, timedelta


def _windowed_list_all_filings(
    scrip_code: str,
    specs,
    years: int,
    client,
    *,
    everything: bool = False,
    _dependencies=None,
):
    if _dependencies is None:
        from engine.errors import FilingForgeError
        from engine.fetcher import ANN_URL, _attachment_id, _classify, list_annual_reports
        from engine.models import Filing
    else:
        FilingForgeError = _dependencies["FilingForgeError"]
        ANN_URL = _dependencies["ANN_URL"]
        _attachment_id = _dependencies["attachment_id"]
        _classify = _dependencies["classify"]
        list_annual_reports = _dependencies["list_annual_reports"]
        Filing = _dependencies["Filing"]

    newest_date = date.today()
    oldest_date = newest_date - timedelta(days=365 * years - 1)
    filings = []
    seen_news_ids: set[str] = set()
    seen_attachments: set[str] = set()
    window_end = newest_date

    while window_end >= oldest_date:
        window_start = max(oldest_date, window_end - timedelta(days=364))
        base = {
            "strCat": "-1",
            "subcategory": "-1",
            "strSearch": "P",
            "strType": "C",
            "strScrip": str(scrip_code),
            "strPrevDate": window_start.strftime("%Y%m%d"),
            "strToDate": window_end.strftime("%Y%m%d"),
        }
        for page_number in range(1, 51):
            rows = client.get_json(ANN_URL, {**base, "pageno": str(page_number)}).get("Table") or []
            if not rows:
                break
            for row in rows:
                hit = _classify(row, specs, everything)
                attachment = str(row.get("ATTACHMENTNAME") or "").strip()
                if hit is None or not attachment:
                    continue
                news_id = str(row.get("NEWSID") or attachment)
                attachment_id = _attachment_id(attachment)
                if news_id in seen_news_ids or attachment_id in seen_attachments:
                    continue
                folder, category = hit
                filings.append(Filing(
                    news_id=news_id,
                    date=str(row.get("DissemDT") or "")[:10],
                    headline=str(row.get("HEADLINE") or row.get("NEWSSUB") or "").strip(),
                    attachment=attachment,
                    folder=folder,
                    category=category,
                ))
                seen_news_ids.add(news_id)
                seen_attachments.add(attachment_id)
        window_end = window_start - timedelta(days=1)

    if everything or any(spec.key == "annual_report" for spec in specs):
        announcement_annual_dates = {
            filing.date for filing in filings if filing.folder == "annual-reports"
        }
        try:
            archived = list_annual_reports(scrip_code, client, years=years)
        except FilingForgeError:
            archived = []
        for filing in archived:
            attachment_id = _attachment_id(filing.attachment)
            if attachment_id in seen_attachments or filing.date in announcement_annual_dates:
                continue
            filings.append(filing)
            seen_attachments.add(attachment_id)

    filings.sort(key=lambda filing: (filing.date, filing.news_id), reverse=True)
    return filings


def install_year_window_fetcher() -> None:
    import engine.library

    engine.library.list_all_filings = _windowed_list_all_filings
