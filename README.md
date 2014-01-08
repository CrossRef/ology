# ology

A tool for getting info out of CrossRef resolution logs. Stores aggregates but throws away the original data (around 100 million entries per month). Data stored is doi per subdomain per day, week and month.

## Usage

To insert log files, run on the command line with the list of log files. The log files can cover any time period, but for that period (i.e. the interval of the earliest and latest date in the supplied log files), all log files that continuously represent that time period should be supplied. Upon insertion, the portion of the aggregate table for that date range will be cleared and re-built. This ensures that the numbers are correct if you enter the same month twice.

Suggested use is to insert all logs for a given month per batch.

To insert a log file into the database

    lein run «path to log file»*

or 

    java -jar /path/to/jar.jar «path to log file»*

## TODO

Are there boundary issues caused by timezones?