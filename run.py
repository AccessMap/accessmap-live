import schedule
import time

import fetchers
import rebuild

#
# Schedule tasks
# Note: order is important!
#

# Fetch + update data tables
schedule.every().day.do(fetchers.construction)

# Update tables based on new data
schedule.every().day.do(rebuild.sidewalks)

# Rebuild routing table + routing graph
schedule.every().day.do(rebuild.routing)

#
# Run the scheduled tasks
#

# Do a fetch on first run
schedule.run_all()

while True:
    schedule.run_pending()
    time.sleep(1)
