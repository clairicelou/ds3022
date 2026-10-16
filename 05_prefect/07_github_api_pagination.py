"""
Work with Claude Code (CC) to complete this prefect flow.

The best way to do this is to open CC in the root dir of this
project, and your prompt can reference @05_prefect/07_github_api_pagination.py
to ask for a plan for how to complete the steps. the instructors
inline comments already add to the prompt - i.e. to paginate over that
API endpoint for a certain number of pages, and to store the output
in a single duckdb table. Ask CC to enumerate the steps you will need
to do this, and REQUIRE it to work with you on each task, one by one.

Read what CC generates/suggests with each step, and ask yourself if you
understand the python it has generated. IF YOU DO NOT UNDERSTAND IT WELL,
this is your chance to learn a bit more python! Ask CC to explain the specific
lines that are unclear to you, and have a back and forth with CC until you
get a sense for what is going on. You know about loops, and you know about
fetching remote web content, and so the growth here is understanding how
to cycle through a series of related links to build up all 1000 records.
"""

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
import duckdb
import httpx
import pandas as pd
from prefect import flow, task
from prefect.logging import get_run_logger
from prefect.task_runners import ThreadPoolTaskRunner


# commit history for the git/git repository
TOKEN = os.environ["GH_PAT"]
HEADERS = {"Authorization": f"Bearer {TOKEN}"}
URL = "https://api.github.com/repos/git/git/commits"
PER_PAGE = 100
PAGES = 10

# mirror every Prefect run log line (task/flow logs, print() via log_prints, retry notices)
# into a local file next to this script; *.log is already gitignored
LOG_FILE = Path(__file__).with_suffix(".log")   # 05_prefect/07_github_api_pagination.log
_fh = logging.FileHandler(LOG_FILE)
_fh.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(name)s - %(message)s"))
logging.getLogger("prefect").addHandler(_fh)


# ------ only one task ------
# create a single prefect task that loops through 10 pages
# of git/git commits and stores each one into a duckdb table.
# add more tasks and more data if you like, and do more with it.
# if successful you should have 1000 records in your db.

# local duckdb file, created if missing
conn = duckdb.connect("actions.duckdb")


def flatten_commit(c: dict) -> dict:
    """Turn one nested GitHub commit dict into a flat row.
    `or {}` / `or []` guard against keys that are missing or explicitly None."""
    info = c.get("commit") or {}
    author = info.get("author") or {}
    committer = info.get("committer") or {}
    return {
        "sha": c.get("sha"),
        "author_name": author.get("name"),
        "author_email": author.get("email"),
        "author_date": author.get("date"),
        "committer_name": committer.get("name"),
        "committer_date": committer.get("date"),
        "message": info.get("message"),
        "github_login": (c.get("author") or {}).get("login"),  # None if no linked GitHub account
        "html_url": c.get("html_url"),
        "parent_count": len(c.get("parents") or []),           # 2+ means a merge commit
    }


@task(name="ingest", log_prints=True, retries=3, retry_delay_seconds=5)
def ingest_data():
    # work with claude to fetch 100 records for each page
    # and pull 10 pages total. This could be done in series,
    # or if you want to look at previous prefect .map examples
    # you could pull some/all pages concurrently.
    #
    # for help with github api pagination see
    # https://docs.github.com/en/rest/using-the-rest-api/using-pagination-in-the-rest-api?apiVersion=2026-03-10
    #
    # httpx parses it into response.links. The "next" URL already includes 
    # per_page and page, so params are only needed on the first request. 
    # The loop ends when there is no "next" link (last page).

    # this task already comes with a name, retry settings, and anything
    # you print() is written to the log.
    # visit https://sds-prefect.pods.uvarc.io/v2/runs to review your flows

    # if you need to see a working example of this, see this file in this repo
    # 04_apis/05_pagination.py

    logger = get_run_logger()
    pages_fetched = 0  # defined before try so the except message can always use it

    try:
        # Step 3: follow GitHub's "next" links until there are none or PAGES is reached
        all_commits = []                 # one flat list of commit dicts across all pages
        url = URL                        # first request uses the base endpoint
        params = {"per_page": PER_PAGE}  # only needed on the first request

        while url and pages_fetched < PAGES:
            response = httpx.get(url, headers=HEADERS, params=params, timeout=30)
            response.raise_for_status()               # turn 4xx/5xx into an exception so retries kick in
            all_commits.extend(response.json())       # add this page's commits individually
            pages_fetched += 1
            url = response.links.get("next", {}).get("url")  # None on the last page
            params = None                             # "next" URL already has per_page & page

        # Step 4: flatten each nested commit into a simple row
        records = [flatten_commit(c) for c in all_commits]

        # Step 5: load the flat rows into one DuckDB table, rebuilt on every run
        df = pd.DataFrame(records)       # 10 columns, one row per commit
        conn.register("records_df", df)  # expose the DataFrame to SQL by name
        conn.execute("""
            CREATE OR REPLACE TABLE commits AS
            SELECT sha, author_name, author_email,
                   CAST(author_date AS TIMESTAMP)    AS author_date,
                   committer_name,
                   CAST(committer_date AS TIMESTAMP) AS committer_date,
                   message, github_login, html_url, parent_count
            FROM records_df
        """)
        conn.unregister("records_df")    # drop the temporary name; the table persists

        row_count = conn.execute("SELECT COUNT(*) FROM commits").fetchone()[0]
        logger.info(f"rows in commits table: {row_count}")
    except Exception as e:
        logger.error(f"ingest failed after {pages_fetched} page(s): {type(e).__name__}: {e}")
        raise  # re-raise so Prefect marks the attempt failed and retries


# name the flow something you can identify as yourself
@flow(name="pnu9pt-flow")
def main_flow():
    ingest_data()

# it doesn't seem this flow executes when you run it. Why?
# fix it ;^)
# the file only *defined* main_flow; nothing called it. This guard runs the flow
# when the file is executed directly, but not when it is imported elsewhere.
if __name__ == "__main__":
    main_flow()
