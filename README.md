This script enables you to pull "log level data" files from the AppNexus API.  

It is suitable for running on a server or on an individual workstation (e.g. an Ad-Ops person can pull a specific hour of data and grep for a specific event for troubleshooting purposes).

You must have the following in order to use this script:

- AppNexus account (aka "seat")
- Log Level Data enabled for your account
- A API enabled user/password.

See the AppNexus documentation for more details about their API.

# Usage

## Create config file

Create `pulllogleveldata-config` file and place it in the same directory as the script.

```
[LoginData]
username: apiuser
password: foobar
memberId: 911

[Paths]
dataDir: ./data

[RateLimiting]
requestsPerMin: 25
```

## Run it

```
python pulllogleveldata.py -d [directoryForLogFiles] -f [filter]
```

e.g.:  `python pulllogleveldata.py -d "~/an-data/" -f "standard_feed"`
will save files to an-data directory and only download files that have path/name matching standard_feed.  So you can easily filter to a specific feed or specific date or specific hour.

## Other notes

The script checks checksums against any existing files in the specified directory to avoid downloading the same file twice.  Only new/changed files are downloaded.

We are running this with cron to do a daily sync/archive of all available log-level data files.
