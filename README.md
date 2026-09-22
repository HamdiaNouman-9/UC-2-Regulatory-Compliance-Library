# Regulatory Compliance Library — pipeline

Crawls regulators' websites, stores their documents in MSSQL, detects what is
new / modified / missing, and (optionally) runs LLM analysis on them. Runs as a
FastAPI service plus scheduled monitor jobs.

## 1. Setup — what `pip install` does NOT install

`pip install -r requirements.txt` installs the Python packages only. **Four things
are installed separately, and the pipeline breaks without the first three.**

```
python -m venv venv
venv\Scripts\activate                 # Linux: source venv/bin/activate
pip install -r requirements.txt       # Python 3.11
```

### 1. Chromium for Playwright — REQUIRED

```
python -m playwright install chromium
```

Re-run it whenever the pinned `playwright` version changes. The browser build must
match the package. **If it does not, every browser crawl silently returns 0
documents and the run still reports success**

### 2. Tesseract OCR with the Arabic language pack — REQUIRED

Scanned Arabic PDFs are read by OCR. Without the `ara` pack they come out empty.

| | |
|---|---|
| Windows | Install Tesseract, then add `ara.traineddata` to its `tessdata` folder (or a user folder) |
| Linux | `apt-get install tesseract-ocr tesseract-ocr-ara` |

Settings (all optional, read from the environment):

| Variable | Default | Use |
|---|---|---|
| `TESSERACT_PATH` | `C:\Program Files\Tesseract-OCR\tesseract.exe` (Windows), `/usr/bin/tesseract` (Linux) | Where the executable is |
| `TESSDATA_PREFIX` | Tesseract's own folder | Point at a user-owned `tessdata` if the install folder is not writable |
| `OCR_LANGS` | `ara+eng` | Languages missing from the install are dropped with a warning |

Check (uses `TESSERACT_PATH`, so it works even when `tesseract` is not on the PATH):

```
python -c "from processor.Text_Extractor import OCRProcessor; print(OCRProcessor.installed_languages())"
```

The list must contain `ara`.

### 3. ODBC Driver 17 for SQL Server — REQUIRED

Install "Microsoft ODBC Driver 17 for SQL Server" (Linux package: `msodbcsql17`).

Then in `.env` 

```
MSSQL_SERVER=...
MSSQL_DATABASE=...
MSSQL_DRIVER={ODBC Driver 17 for SQL Server}
MSSQL_USERNAME=...      # leave both blank to use Windows authentication
MSSQL_PASSWORD=...
OPENROUTER_API_KEY=...  # only for LLM analysis
```

Check: `python -c "import pyodbc; print(pyodbc.drivers())"` must list the driver.

### 4. sentence-transformers - REQUIRED

```
pip install sentence-transformers==5.6.0    
```

- Pulls in `torch` (~2 GB) and downloads the `all-MiniLM-L6-v2` model (~90 MB) on
  first use. On an offline server, pre-populate the Hugging Face cache (`HF_HOME`).
- **Do not install it on a server that cannot reach the model.**
  `crawler/smart_matcher.py` loads the model at import time; if the package is
  present but the download fails, the import crashes.

## 2. Run

```
python -m uvicorn apis.pipeline_api:app --port 8000
```

- **Run a single worker.** Job state is held in memory and jobs run as threads
  inside this process.
- The API has **no authentication** and allows any origin. Put it behind your own
  auth before exposing it.

Trigger one or several regulators (returns 202 immediately; runs take 5 seconds to
over an hour):

```
GET  /trigger/regulators            # every regulator, its job, and status
POST /trigger/regulators            # {"regulators": ["CMA", "MC"]}
GET  /trigger/regulators/status     # job state and result summary (in memory)
```

Monitor jobs are timed in `config/scheduler.yml` (`scheduler/scheduler.py` runs
them). A job that is `enabled: false` there can still be triggered through the API.
All monitor jobs share one crawl lock (`output/monitor_jobs.lock`), so only one
runs at a time and a second is skipped.

## 3. Verify a new install

```
pip check
python -c "from playwright.sync_api import sync_playwright as p; x=p().start(); b=x.chromium.launch(); print('chromium OK', b.version); b.close(); x.stop()"
python -c "from processor.Text_Extractor import OCRProcessor; print(OCRProcessor.installed_languages())"
python -c "import pyodbc; print(pyodbc.drivers())"
curl http://127.0.0.1:8000/trigger/regulators
```

Expect: no broken requirements; `chromium OK`; `ara` in the OCR list; the ODBC
driver in the last list; and the regulator list from the API. (`playwright install
--dry-run` only prints what it *would* download, so it does not prove the browser
works.)

## 4. SIMAH and Saudi Exchange (blocked hosts)

Both sites blocked this project after repeated automated visits during development.
Their jobs read a **saved page** and cannot visit the site unless `allow_live` is
true in `config/sources/simah.yml` / `saudi_exchange.yml` (it ships `false`).

`output/` is not in git, so **a new machine has no saved pages**. Follow the order
in those two config files: approve the form, take ONE visit by hand, and only if it
succeeds set `allow_live: true` and enable the scheduler slot.

```
python -m dynamic_crawler.formfill snapshot dynamic_crawler/hints/simah.rules.yml
python -m dynamic_crawler.formfill snapshot dynamic_crawler/hints/simah.rules.yml --status   # no request
```
