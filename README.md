# Overview

This repo contains a web server that regularly downloads, processes, and
updates data in the AccessMap database.

The type of data that needs updating includes temporal data (e.g. construction
information that needs to be downloaded on a regular basis) as well as any
downstream tables that need to be updated as a result (like the routing table,
which needs to know about construction impacting sidewalks).

There is probably a nice, clean, well-designed implementation we could follow
to make this happen, but this repo is currently about making something that
(barely) works and redesigning when we need to scale up past Seattle and the
requirements are more clear.

# Implementation

Regularly incorporating new data into the database involves several steps.
Construction data that impacts the sidewalk is a good example:

1. The data needs to be updated regularly, as new construction permits are
added by SDOT.

2. The data is not directly usable and requires significant post-processing to
determine the actual predicted locations.

The first process, regularly updating the database, has not existed for
AccessMap before, and is highly experimental.

The current implementation downloads a new dataset from data.seattle.gov
(restricting size by requesting only the construction permits that apply to
the current date), processes it in Python using geopandas, and overwrites
the existing `live.construction` table. This should cause an immediate, live
update to all construction data displayed on AccessMap.

# Installation

This project is currently written in Python 3 so as to make use of convenient
GIS tools (like GeoPandas).

### Step 1: Install Python 3

Install Python 3 and make it available in your `PATH` if it isn't already.

### Step 1b (optional): Use a virtual environment

Using a virtual environment will isolate the server's Python installation
environment.

`python3 -m venv venv && source venv/bin/activate`

### Step 2: Install the required Python libraries

`pip install -r requirements.txt`

# Running the server

These steps need to be run every time you open a new terminal to run the
server.

### Step 1: Put `DATABASE_URL` in your `PATH`

`accessmap-live` needs a database connection URI with write permissions to the
tables of interest (currently the `public` schema) in order to work. The
`set_envs.sh.example` file helps with a convenient workflow. First, copy the
file to `set_envs.sh`:

`cp set_envs.sh.example set_envs.sh`

Then edit the URI to match your database.

Second, source `set_envs.sh` so that `DATABASE_URL` is in your path:

`source set_envs.sh`

### Step 2: Run the server

If you used a virtual environment, enable it with `source venv/bin/activate`

Then run the server:

`python run.py`
