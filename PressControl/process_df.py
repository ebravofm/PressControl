from multiprocessing import Pool
from PressControl.process_link import process_link
from PressControl.utils import mysql_engine, read_config, tprint
import pandas as pd
import numpy as np
import pickle
from contextlib import closing



def get_full_df(n_pools=15, n=150, 
                queue_table=read_config()['queue_table'],
                processed_table=read_config()['processed_table'], 
                delete=False,
                engine=mysql_engine(), 
                con=None,
                rand=False):
    
    chunk = get_chunk_from_db(n, queue_table, processed_table, delete, engine, con, rand)
    
    df = populate_df(chunk, n_pools)
    
    return df


def populate_df(df, n_pools=15):
    pd.options.mode.chained_assignment = None
    
    row_list = list(df.T.to_dict().values())
    

    with closing( Pool(15) ) as p:
        news_dict = p.map(process_row, row_list, 15)

    out = pd.DataFrame(news_dict)
    
    # Set Dummy variables to 0 instead of None
    
    try:
        out.borrar[out.borrar.isnull()] = 0
    except:
        pass
    return out


def process_row(row):
    d = process_link(row['original_link'])

    d['original_link'] = row['original_link']
    
    fields = ['titulo', 'bajada', 'contenido', 'autor', 'fecha', 'seccion', 'fuente', 'ano', 'imagen', 'error', 'borrar', 'tags', 'link', 'info']
    
    for f in fields:
        if f not in d.keys(): d[f] = None
    
    return d


def get_chunk_from_db(n=150, 
                    queue_table=read_config()['queue_table'], 
                    processed_table=read_config()['processed_table'], 
                    delete=False, 
                    engine=mysql_engine(), 
                    con=None,
                    rand=False):
    if con == None:
        con = engine.connect()
        
    order = ''
    if rand == True:
        order = 'order by rand()'
    
    query = f'select original_link from {queue_table} {order} limit {str(n)}'
    
    try:
        # Reading rows
        df = pd.read_sql(query, con)
        
        # Backup and delete rows
        if delete == True:
            df.to_sql(processed_table, con = con, if_exists='append', index=False)
            engine.execute('delete from '+queue_table+' limit '+str(n))
        

    except Exception as exc:
        tprint('[-] Error en get_chunk_from_db()', exc)
        df = None
        
    return df


