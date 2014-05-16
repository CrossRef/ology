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


Indexes are dropped prior to insertion. Run this in the mongo shell after:

    db['aggregated-doi-day'].ensureIndex(  {'o': 1, 'y': 1, 'm': 1, 'a': 1}, {'background': true});
    db['aggregated-doi-month'].ensureIndex({'o': 1, 'y': 1, 'm': 1, 'a': 1}, {'background': true});

    db['aggregated-domain-day'].ensureIndex(  {'s': 1, 'n': 1, 'l': 1, 'y': 1, 'm': 1, 'a': 1}, {'background': true});
    db['aggregated-domain-month'].ensureIndex({'s': 1, 'n': 1, 'l': 1, 'y': 1, 'm': 1, 'a': 1}, {'background': true});

    db['aggregated-domain-doi-day'].ensureIndex(  {'o': 1, 's': 1, 'n': 1, 'l': 1, 'y': 1, 'm': 1, 'a': 1}, {'background': true});
    db['aggregated-domain-doi-month'].ensureIndex({'o': 1, 's': 1, 'n': 1, 'l': 1, 'y': 1, 'm': 1, 'a': 1}, {'background': true});

If there was a bad run, you can delete data from a month in the mongo console.

    var delMonth = function(year, month) {
        db['aggregated-doi-day'].remove({'y': year, 'm': month});
        db['aggregated-doi-month'].remove({'y': year, 'm': month});
        db['aggregated-domain-day'].remove({'y': year, 'm': month});
        db['aggregated-domain-month'].remove({'y': year, 'm': month});
        db['aggregated-domain-doi-day'].remove({'y': year, 'm': month});
        db['aggregated-domain-doi-month'].remove({'y': year, 'm': month});
    }

Then

    delMonth(2013, 6)

### Run server for querying

    lein ring server-headless
