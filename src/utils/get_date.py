import datetime
import numpy as np


def get_date():
    # Creates date string in format 'MMDDYYYY'

    return datetime.date.today().strftime("%m%d%Y")


def convert_to_timestamp(timestamp: str):
    timestamp = int(np.ceil(int(timestamp) / 1000))
    return timestamp
