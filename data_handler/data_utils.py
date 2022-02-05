
from datetime import datetime

def isin_future(date):
    now = datetime.now()
    date = datetime.strptime(date, "%Y-%m-%d")
    return date > now

