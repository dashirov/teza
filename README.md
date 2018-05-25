# teza

usage: sms.py [-h] [--debug] [--nexus] [--version] [--config CONFIG]
              [--data DATA] [--file-name-pattern FILE_NAME_PATTERN]
              [--db-dsn DB_DSN] [--db-table DB_TABLE]

SMS Data Loader

optional arguments:
  -h, --help            show this help message and exit
  --debug               Print debugging information to stdout
  --nexus               Append data lineage column to exported data
  --version             Print version and exit
  --config CONFIG       Location of the configuration file
  --data DATA           Location of the data archive (zip file)
  --file-name-pattern   SMS file name pattern
                        Pattern basename a zipped object must match in order
                        to be considered for processing
  --db-dsn DB_DSN       Database DSN
  --db-table DB_TABLE   Database Table name
