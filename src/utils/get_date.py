import datetime

def get_date():
    Day = datetime.date.today().strftime("%d")
    Mo = datetime.date.today().strftime("%m")
    Yr = datetime.date.today().strftime("%Y")

    DateToday = "{}{}{}".format(Mo, Day, Yr)  # Creates date string in format 'DDMMYYYY'
    print(DateToday)

    return DateToday
