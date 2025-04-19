import datetime


def get_date():
    # Creates date string in format 'MMDDYYYY'

    return datetime.date.today().strftime("%m%d%Y")
