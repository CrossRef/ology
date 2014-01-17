# ology

A tool for getting info out of CrossRef resolution logs. Stores aggregates but throws away the original data (around 100 million entries per month). Data stored is doi per subdomain per day, week and month.

## Usage

### Insert log files

To insert log files, run on the command line with the list of log files. Within a log file, the entries must be in date order. The aggregation allows for the logs to be intered in any order, but you must ensure you don't ever give it the same log file twice, or the numbers will come out wrong.

To insert a log file into the database

    lein run «path to log file»*

or 

    java -jar /path/to/jar.jar «path to log file»*

### Run server for querying

    lein ring server-headless
