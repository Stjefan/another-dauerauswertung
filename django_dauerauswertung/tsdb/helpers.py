from datetime import datetime, timedelta


def get_beurteilungszeitraum_start(arg: datetime):
    if 6<= arg.hour <= 21:
        return datetime(arg.year, arg.month, arg.day, 6, 0, 0), datetime(arg.year, arg.month, arg.day, 21, 59, 59)
    else:
        return datetime(arg.year, arg.month, arg.day, arg.hour, 0, 0), datetime(arg.year, arg.month, arg.day, arg.hour, 0, 0) + timedelta(hours=1, seconds=-1)