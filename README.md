## Introduction
This script will facilitate droping old tables using data retention policies and exporting tables and its partitions to csv files.

## Usage Examples
```
usage: questdb_data_retention.py [-h] [--csv] [--output-folder OUTPUT_FOLDER] [--dont-drop] [-u UNIT] [-n TIME_UNIT] [-H HOST] [-f] table

Facilitates exporting table partitions to .csv and dropping old ones

positional arguments:
  table                 Specify the table used for queries, e.g. "sensorData".

optional arguments:
  -h, --help            show this help message and exit
  --csv                 Save partitions to csv file, requires specifying a destination folder which will be created recursively if it doesn't exist.
  --output-folder OUTPUT_FOLDER, -o OUTPUT_FOLDER
                        .csv file destinaton folder. Only required if the --csv is set. Defaults to $HOME/<table>/
  --dont-drop           Will not drop found partitions. Use the --csv option if you want to keep your data. Default if False, so by default your data will be deleted.
  -u UNIT, --unit UNIT  Set time unit for partition search, default is 'd' for days.
  -n TIME_UNIT, --time-unit TIME_UNIT
                        Amount to keep. Partitions with minTimestamp older than now() minus the number specified here will be selected for deletion and/or exporting to csv.
  -H HOST, --host HOST  Specify the database host, defaults to "http://127.0.0.1:9000".
  -f, --force           Don't ask for confirmation to drop partitions and/or overwrite existing files.

Created with love by LightFTSO | admin@lightft.so
```
**First, let's make the script executable**:
`` $: chmod +x ./questdb_data_retention.py``

 - Save partitions older than 7 days to .csv files in the Users's home folder **AND DROP THE PARTITIONS**
 
``
$ ./questdb_data_retention.py --csv --out-folder /home/User/ --host http://192.168.1.201:9000 <table_name> -n 7
``
 - Save partitions older than 7 days to .csv files in the Users's home folder **AND DONT DROP THE PARTITIONS**
 
``
$ ./questdb_data_retention.py --csv --out-folder /home/User/ --dont-drop --host http://192.168.1.201:9000 <table_name> -n 7
``
 - **DROP** partitions older than 30 days (this will **NOT** save .csv files, **DATA WILL BE LOST**)
 
``
$ ./questdb_data_retention.py --host http://192.168.1.201:9000 <table_name>
``

 - Add to crontab, removes tables older than 30 days, executes every day at 01:00 AM. *Use the ``--force``option to not ask for confirmation*
 
``
0 1 * * * ./questdb_data_retention.py --host http://192.168.1.201:9000 -f <table_name>
``
