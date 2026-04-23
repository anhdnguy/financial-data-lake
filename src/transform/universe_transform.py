from datetime import datetime
import pytz

def _get_today():
    now = datetime.now()
    current_date = now.date()
    return current_date