from os import path, getcwd
import sys
import argparse
import configparser
import sqlalchemy
from zipfile import ZipFile
import re
import pandas
# TODO: replace print statements with proper logging
import logging as logger
import numpy


class SMSDataLoader(object):
    def main(self, basedir):
        try:
            bindir = path.abspath(path.dirname(sys.argv[0]))
            cfgdir = path.abspath(path.join(bindir, '..', 'etc'))
            parser = argparse.ArgumentParser(description='SMS Data Loader')

            parser.add_argument(
                '--debug', action="store_true", help='Print debugging information to stdout', default=None)
            parser.add_argument(
                '--nexus', action='store_true', help='Append data lineage column to exported data', default=None)
            parser.add_argument(
                '--version', action='store_true', help='Print version and exit', default=None)
            parser.add_argument(
                '--config', action='store', help='Print version and exit', default=None)
            parser.add_argument(
                '--data', action='store', help='Location of the data archive (zip file)', default=None)
            parser.add_argument(
                '--file-name-pattern', action='store',
                help='Pattern basename a zipped object must match in order to be considered for processing',
                default='social_media_signal_[0-9]{6}\.csv$')
            parser.add_argument(
                '--db-dsn', action='store', help='Database DSN', )
            parser.add_argument(
                '--db-table', action='store', help='Database Table name')
            args = parser.parse_args()

            config = configparser.ConfigParser(raw=False, safe=True)
            realScript = path.basename(path.split(__file__)[0])
            defaultConfig = path.abspath(path.join(cfgdir, realScript + '.cfg'));
            if args.config is not None:
                assert path.isfile(args.config), "Config file {} does not exist".format(args.config)
                config.read(args.config)
            elif path.isfile(defaultConfig):
                config.read(defaultConfig)
            if args.debug is None:
                if config.has_option('main', 'debug'):
                    args.debug = config.get('main', 'debug')
                else:
                    args.debug = False
            if args.debug is True:
                print 'Debug to console: {}'.format(args.debug)

            if args.nexus is None:
                if config.has_option('main', 'nexus'):
                    args.nexus = config.get('main', 'nexus')
                else:
                    args.nexus = True
            if args.debug is True:
                print 'Nexus to data: {}'.format(args.nexus)

            if args.db_dsn is None:
                if config.has_option('main', 'db-dsn'):
                    args.db_dsn = config.get('main', 'db-dsn')
                else:
                    args.db_dsn = 'redshift+psycopg2://scott:tiger@dw.c55glynjrruj.us-east-1.redshift.amazonaws.com:5439/dev'
                if args.debug is True:
                    print 'DB DSN: {}'.format(args.db_dsn)

            if args.db_table is None:
                if config.has_option('main', 'db-table'):
                    args.db_table = config.get('main', 'db-table')
                else:
                    args.db_table = 'imported.some_table'
            if args.debug is True:
                print 'DB Table: {}'.format(args.db_table)

            if args.data is None:
                if config.has_option('main', 'data'):
                    args.data = config.get('main', 'data')
                else:
                    args.data = None
            if args.debug is True:
                print 'Data file (zip archive): {}'.format(args.data)
            assert args.data is not None

            if args.file_name_pattern is None:
                if config.has_option('main', 'file-name-pattern'):
                    args.file_name_pattern = config.get('main', 'file-name-pattern')
                else:
                    args.file_name_pattern = '^.*$'
            if args.debug is True:
                print 'File name pattern: {}'.format(args.file_name_pattern)
            args.file_name_pattern = re.compile(args.file_name_pattern)

            # Connect to DB - most likely first thing to fail
            eng = sqlalchemy.create_engine(args.db_dsn)
            connection = eng.connect()

            # Connect to inputs - next most likely thing to fail
            priceFileName = None
            smsFileNames = []
            priceDataFrame = None

            smsDataFrame = None
            smFileInfo = pandas.DataFrame(columns=['smFile', 'rowsSeen', 'rowsIngested'])

            with ZipFile(args.data, 'r') as myzip:
                for file in myzip.infolist():
                    if args.debug is True:
                        print("Checking {}".format(file.filename))
                    (head, tail) = path.split(file.filename)
                    if ((priceFileName is None) and ("price_data.csv" == tail)):
                        priceFileName = file.filename
                    elif args.file_name_pattern.match(tail):
                        smsFileNames.append(file.filename)
                    else:
                        if args.debug is True:
                            print "Not matched!"
                assert len(smsFileNames) > 0
                assert priceFileName is not None
                with  myzip.open(priceFileName, 'r') as priceDataFile:
                    priceDataFrame = pandas.read_csv(priceDataFile, sep="|", index_col=False)
                for smsFile in smsFileNames:
                    if args.debug is True:
                        print "Loading {}".format(smsFile)
                    with myzip.open(smsFile, 'r') as smsDataFile:
                        tmpDataFrame = pandas.read_csv(smsDataFile, sep=', ', usecols=range(4), skipinitialspace=True,
                                                       index_col=False,
                                                       warn_bad_lines=True, error_bad_lines=False, engine='python')
                        cntOriginal = len(tmpDataFrame.index)
                        tmpDataFrame.rename(index=str, columns={"daet": "date"}, inplace=True)
                        if args.debug is True:
                            print(tmpDataFrame.columns.values)
                        tmpDataFrame.drop(
                            tmpDataFrame[(tmpDataFrame.tweets.isnull() | tmpDataFrame.positive.isnull())].index,
                            inplace=True)
                        tmpDataFrame.drop_duplicates(subset={"date", "ticker"}, keep="first", inplace=True)
                        tmpDataFrame['tweets'] = tmpDataFrame['tweets'].astype(numpy)
                        tmpDataFrame = tmpDataFrame[['date', 'ticker', 'positive', 'tweets']]

                        if args.nexus is True:
                            tmpDataFrame['nexus'] = smsFile

                        if args.debug is True:
                            print(tmpDataFrame.columns.values)

                        cntFinal = len(tmpDataFrame.index)
                        smFileInfo.loc[len(smFileInfo.index)] = [smsFile, cntOriginal, cntFinal]
                        if smsDataFrame is None:
                            smsDataFrame = tmpDataFrame
                        else:
                            smsDataFrame.append(tmpDataFrame, sort=True, verify_integrity=True, ignore_index=True)
            smsDataFrame.drop_duplicates(subset={"date", "ticker"}, keep="first", inplace=True)
            sentimentOnPrice = pandas.merge(smsDataFrame, priceDataFrame,
                                            left_on=['ticker', 'date'],
                                            right_on=['ticker', 'date'],
                                            how='inner')
            sentimentOnPrice["date"] = pandas.to_datetime(sentimentOnPrice['date'], format='%Y-%m-%d')
            sentimentOnPrice.to_sql(args.db_table, eng, if_exists="replace", index=False, chunksize=1000)

        except KeyError, e:
            message = "Processing of {0} failed: {1}".format(args[0], str(e.message))


if __name__ == '__main__':
    feed_handler = SMSDataLoader()
    feed_handler.main(getcwd())
    exit(code=0)
