Fill in userId, password and memberId for your appnexus account and run.

python pulllogleveldata.py -d [directoryForLogFiles] -f [filter]

e.g.:  python pulllogleveldata.py -d "~/an-data/" -f "standard_feed"
will save files to an-data directory and only download files that have path/name matching standard_feed.

The script checks checksum on any existing files in the specified directory to avoid downloading
the same file twice.  This also allows it to easily redownload files that have changed on the server.

We are running this out of crontab to do a daily sync/archive of all available log-level data files.

This is also handy for pulling one specific file for analysis (pull a specific hour using the filter and
then grep through it for specific impressions/clicks/etc).
