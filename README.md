# ology

A tool for getting info out of CrossRef resolution logs. Stores aggregates but throws away the original data (around 100 million entries per month). Data stored is doi per subdomain per day, week and month.

This can deal with any size of input. Typically one month's worth of log files are over 10GB. The entries are parsed and binned by date and then frequencies are calculated an inserted by date. The process leaves its temporary files behind for verification, but it clears the directory before starting. You can run it with /dev/null as a log file input to delete the directory.

## Usage

### Insert log files

To insert log files, run on the command line with the list of log files. Within a log file, the entries must be in date order. The aggregation allows for the logs to be intered in any order. For each date mentioned (year/month/day) the numbers are calculated and replaced in the database. It's best to run this on one month (or groups of one-month) files in a batch. You must specify a temporary directory as the first argument.

To insert a log file into the database

    lein run «path to temporary directory» «path to log file»*

or 

    java -jar /path/to/jar.jar «path to temporary directory» «path to log file»*

### Run server for querying

    lein ring server-headless
