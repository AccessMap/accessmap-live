import schedule
import time

from . import fetchers

schedule.every().day.do(fetchers.construction())

while True:
    schedule.run_pending()
    time.sleep(1)
