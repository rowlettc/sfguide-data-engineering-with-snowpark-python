from datetime import datetime, timedelta

def datetime_range(start, end, delta):
    current = start
    while current < end:
        yield current
        current += delta

dts = [dt.strftime('%Y-%m-%d T%H:%M Z') for dt in 
       datetime_range(datetime(2023, 5, 28, 7), datetime(2023, 5, 28, 9+12), 
       timedelta(minutes=1))]

print(dts)