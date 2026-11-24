import logging
import re
import time

from radar.app import LOG_DATE_FORMAT, LOG_FORMAT


def test_log_lines_start_with_utc_timestamp_and_keep_level_prefix():
    formatter = logging.Formatter(LOG_FORMAT, LOG_DATE_FORMAT)
    formatter.converter = time.gmtime
    record = logging.LogRecord(
        "__main__", logging.ERROR, __file__, 1, "collector x failed", None, None
    )
    record.created = 1_764_000_000.0  # 2025-11-24T16:00:00Z

    line = formatter.format(record)

    assert line == "2025-11-24T16:00:00Z ERROR:__main__:collector x failed"
    # Health scripts grep "ERROR" and message text; both still match.
    assert re.search(r"ERROR:__main__:collector", line)
