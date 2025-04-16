import datetime

def get_date():
    
    date_today = datetime.date.today().strftime("%m%d%Y")  # Creates date string in format 'MMDDYYYY'

    return date_today
