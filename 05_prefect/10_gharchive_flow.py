import gzip
import json
from collections import Counter
import requests
from prefect import flow, task, get_run_logger


URL = "https://data.gharchive.org/2025-01-01-{hour}.json.gz"


@task(retries=3, retry_delay_seconds=5)
def count_events_for_hour(hour):
    # stream one hourly gzipped file and return {event_type: count}
    logger = get_run_logger()
    counts = Counter()
    bad_lines = 0
    try:
        with requests.get(URL.format(hour=hour), stream=True, timeout=60) as r:
            r.raise_for_status()
            # decompress the byte stream as it arrives; each line is one JSON event
            with gzip.GzipFile(fileobj=r.raw) as gz:
                for line in gz:
                    try:
                        counts[json.loads(line)["type"]] += 1
                    except (json.JSONDecodeError, KeyError):
                        bad_lines += 1  # skip bad or type-less lines instead of failing the hour
    except (requests.RequestException, OSError) as e:
        logger.error(f"hour {hour} failed: {e}")
        raise  # re-raise so Prefect's retries kick in
    if bad_lines:
        logger.warning(f"hour {hour}: skipped {bad_lines} malformed lines")
    print(f"hour {hour}: {counts.total()} events")
    return dict(counts)


@task
def combine_counts(hourly_counts):
    # Total counts by type and order them
    # print those to screen and return them
    pass


@flow(log_prints=True)
def gharchive_flow(hours: list[int] = list(range(1, 6))):
    # take the range and create a prefect .map over the hours
    hourly = count_events_for_hour.map(hours).result()
    totals = combine_counts(hourly)
    for k, v in totals.items():
        print(k, v)


if __name__ == "__main__":
    gharchive_flow()
