This repo contains a web server that regularly downloads, processes, and
updates temporal data in the AccessMap database (the `live` postgres schema).

For example, putting construction information in AccessMap requires two
important processes:

1. The data needs to be updated regularly, as new construction permits are
added by SDOT.

2. The data is not directly usable and requires significant post-processing to
determine the actual predicted locations.

The first process, regularly updating the database, has not existed for
AccessMap before, and is highly experimental.

The current implementation
downloads a new dataset from data.seattle.gov (restricting size by requesting
only the construction permits that apply to the current date), processes it in
Python using geopandas, and overwrites the existing `live.construction` table.
This should cause an immediate, live update to all construction data displayed
on AccessMap.
