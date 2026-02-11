"""
Delran BOE Preschool Monitor – Scraper (Enhanced + Diagnostics, clean)

What it does
------------
1) Crawls the Delran BOE Meeting Minutes and BOE pages for PDF/DOCX/HTML items.
2) Optionally scans BoardDocs Public for PDFs (limited by MAX_BOARDDOCS_FILES).
3) Extracts text and finds preschool-related mentions (via parser_utils.py).
4) Builds HTML + CSV report and emails it monthly.
5) Persists 'seen' match hashes in state.json to dedupe future runs.
6) First run does a backfill from 2021-01-01 to today; then runs monthly.
7) Writes a full audit log of *every* document seen to scanned.csv and
   appends a "Documents scanned" section to the email.
8) If discovery returns 0 links, saves the raw Minutes/BOE HTML and items.json
   under .debug/ to quickly diagnose site/layout changes.

Environment (set in workflow or repo secrets)
---------------------------------------------
- DELRAN_MINUTES_URL   (default: https://www.delranschools.org/b_o_e/meeting_minutes)
- DELRAN_BOE_URL       (default: https://www.delranschools.org/b_o_e)
- BOARDDOCS_PUBLIC_URL (default: https://go.boarddocs.com/nj/delranschools/Board.nsf/Public)

- REPORT_TO                -> recipient (default: robwaz@delrankids.net)
- REPORT_FROM or MAIL_FROM -> sender (one required for sending)
- SMTP_HOST
- SMTP_PORT                -> 587 (STARTTLS) or 465 (SSL)
- SMTP_USER or SMTP_USERNAME
- SMTP_PASS or SMTP_PASSWORD

- STATE_FILE            -> default: state.json
