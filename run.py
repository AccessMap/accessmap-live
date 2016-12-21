import schedule
import time

import fetchers
import rebuild

schedule.every().day.do(fetchers.construction)
schedule.every().day.do(rebuild.routing)

# Do a fetch on first run
schedule.run_all()

while True:
    schedule.run_pending()
    time.sleep(1)
