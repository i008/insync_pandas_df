import threading
import time
import datetime
import dateutil.parser
import pandas as pd
import time

import sys
reload(sys)
# sys.setdefaultencoding("utf-8")

import logging
logger = logging.getLogger()
fhandler = logging.FileHandler(filename='mylog.log', mode='a')
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fhandler.setFormatter(formatter)
logger.addHandler(fhandler)
logger.setLevel(logging.DEBUG)


def threaded(function, daemon):
    """
    Turns given function into a threaded function.
    """
    name = '%s-%d' % (function.__name__, threading.active_count())

    def function_wrapper(*args, **kwargs):
        if '_delay' in kwargs:
            func = delayer(function, kwargs['_delay'])
            del kwargs['_delay']
        else:
            func = function
        thread = threading.Thread(target=func, name=name, args=args, kwargs=kwargs)
        thread.daemon = daemon
        thread.start()
    function_wrapper.__name__ = function.__name__
    return function_wrapper


def daemon(function):
    """
    Turns a function into a threaded function.
    The thread is a daemon thread.
    """
    return threaded(function, True)


class InSyncDf(object):
    
    def __init__(self, table_name, engine):
        self.table_name = table_name
        self.engine = engine
        
    def create_df(self):
        self.df = pd.read_sql(self.table_name, self.engine)
    
    @property
    def df_last_updated(self):
        return sorted(self.df.date)[-1].to_datetime()+datetime.timedelta(0,1) 
    
    @property
    def db_last_updated(self):
        t = self.engine.execute(
            'SELECT date FROM {0} ORDER BY date DESC LIMIT 1'.format(
                self.table_name).fetchone().values()[0]
        return t
    
    def to_be_updated(self):
        rows_to_update = pd.read_sql(
                'SELECT * FROM {0} WHERE date > "{1}"'.format(
                    self.table_name,
                    str(self.df_last_updated)),db)
        return rows_to_update
    
    @daemon
    def start_syncing(self):
        logging.info('in main sync')
        while True:
            if self.df_last_updated < self.db_last_updated:
                time.sleep(1)
                try:
                    rows_to_update = self.to_be_updated()
                except Exception as e:
                    logging.info(str(e))
                self.df = self.df.append(rows_to_update)
            else:
                time.sleep(1)
                
    @property
    def up2date_df(self):
        if self.df_last_updated < self.db_last_updated:
            return self.df.append(self.to_be_updated())
        else:
            return self.df



if __name__ == '__main__':
    from sqlalchemy import create_engine
    db = create_engine('mysql+pymysql://user:pass@host')
    sync_df = InSyncDf('winddata',db)
    sync_df.create_df()
