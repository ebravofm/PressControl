from multiprocessing import Pool
from presscontrol.process_link import process_link
from presscontrol.utils import mysql_engine, tprint
from presscontrol.config import config
import pandas as pd
import numpy as np
import pickle
from contextlib import closing
import signal
import sys


def get_full_df(n_pools=15,
                n=150,
                queue_table=None,
                processed_table=None,
                delete=False,
                engine=None,
                con=None,
                rand=False):
    
    if engine == None:
        engine = mysql_engine()
    if queue_table == None:
        queue_table = config['TABLES']['QUEUE']
    if processed_table == None:
        processed_table = config['TABLES']['PROCESSED']
    
    tprint('[·] Getting chunk...')
    chunk = get_chunk_from_db(n=n,
                              queue_table=queue_table,
                              processed_table=processed_table,
                              delete=delete,
                              engine=engine,
                              con=con,
                              rand=rand)
    
    tprint('[·] Populating chunk...')
    df = populate_df(chunk, n_pools=n_pools)
    
    return df

def init_worker():
    signal.signal(signal.SIGINT, signal.SIG_IGN)

def populate_df(df, n_pools=15):
    pd.options.mode.chained_assignment = None
    
    row_list = list(df.T.to_dict().values())
    
    p = Pool(15, init_worker)
    
    try:
        news_dict = p.map_async(process_row, row_list, 15)
        news_dict.wait()

        out = pd.DataFrame(news_dict.get())
        
        p.close()
        p.join()

        # Set Dummy variables to 0 instead of None

        try:
            out.borrar[out.borrar.isnull()] = 0
        except:
            pass
        return out
    
    except KeyboardInterrupt:
        print()
        tprint('Interrupted')
        p.terminate()
        p.join()
        sys.exit()
        

def process_row(row):
    d = process_link(row['original_link'])

    d['original_link'] = row['original_link']
    
    fields = ['title', 'description', 'body', 'authors', 'date', 'section', 'source', 'year', 'image', 'error', 'borrar', 'tags', 'link', 'info']
    
    for f in fields:
        if f not in d.keys(): d[f] = None
    
    return d


def get_chunk_from_db(n=150, 
                    queue_table=None, 
                    processed_table=None, 
                    delete=False, 
                    engine=mysql_engine(), 
                    con=None,
                    rand=False):
    
    if queue_table == None:
        queue_table = config['TABLES']['QUEUE']
    if processed_table == None:
        processed_table = config['TABLES']['PROCESSED']

    if con == None:
        con = engine.connect()
        
    order = 'order by id'
    if rand == True:
        order = 'order by rand()'
    
    query = f'select original_link from {queue_table} {order} limit {str(n)}'
    
    try:
        # Reading rows
        df = pd.read_sql(query, con)
        
        # Backup and delete rows
        if delete == True:
            df.to_sql(processed_table, con = con, if_exists='append', index=False)
            engine.execute(f'delete from {queue_table} limit {str(n)}')
        

    except Exception as exc:
        tprint('[-] Error en get_chunk_from_db()', exc)
        df = None
        
    return df