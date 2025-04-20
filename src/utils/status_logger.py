from utils.duckdb import execute
import duckdb
from utils.get_date import convert_to_timestamp


def log_status(
    event_id: str, status: str, timestamp: str, conn: duckdb.DuckDBPyConnection
) -> None:
    """
    Update the event_info.txt file with a status and timestamp.

    Args:
        event_id (str): event id
        status (str): event status
        timestamp (str): timestamp
        conn (duckdb.DuckDBPyConnection): duckdb connection
    """

    # need to divide by 1000 to convert miliseconds to seconds
    # also convert to int from float
    timestamp = convert_to_timestamp(timestamp)
    execute(
        conn,
        f"""
        INSERT INTO event_log
        SELECT
            '{event_id}' AS event_id,
            '{status}' AS status,
            TIMESTAMP '1970-01-01 00:00:00' + INTERVAL {timestamp} SECOND AS timestamp;""",
    )


def get_last_status(event_id: str, conn: duckdb.DuckDBPyConnection) -> tuple:
    """
    Read the last status and update time.

    Args:
        event_id (str): event id
        conn (duckdb.DuckDBPyConnection): duckdb connection

    Returns:
        result (tuple): (last status (str), last update timestamp (str))
    """

    result = execute(
        conn, f"SELECT status, timestamp FROM event_log WHERE event_id = '{event_id}'"
    ).fetchall()

    return result[0]
