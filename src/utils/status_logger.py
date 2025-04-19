import os

EVENT_INFO_FILE = "event_info.txt"


def log_status(dir: str, status: str, timestamp: str) -> None:
    """
    Update the event_info.txt file with a status and timestamp.

    Args:
        dir (str): event directory
        status (str): event status
        timestamp (str): timestamp
    """
    f = open(os.path.join(dir, EVENT_INFO_FILE), "w+")
    f.write("{} | {}\n".format(status, timestamp))
    f.close()


def get_last_status(
    dir: str,
) -> tuple:
    """
    Read the last status and update time.

    Args:
        dir (str): event directory

    Returns:
        old_status (str): last status
        old_updated (str): last update timestamp
    """
    # go into folder and read former status and update time
    f = open(os.path.join(dir, EVENT_INFO_FILE), "r")
    status_line = f.readline().rstrip()
    old_status, old_updated = status_line.split(" | ")
    f.close()

    return old_status, old_updated
