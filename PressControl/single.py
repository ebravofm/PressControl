from PressControl.utils import mysql_engine, tprint, read_config
from PressControl.process_df import get_full_df
import pandas as pd
from PressControl.utils import tprint
import time


def work(result_table=None, 
         df=None,
         debug=True,
         n_pools=15, 
         n=150, 
         queue_table=None,
         processed_table=None,
         error_table=None,
         delete=False,
         engine=None, 
         con=None, 
         rand=False,
         config=None):
    
    if df is None:
        df = pd.DataFrame()
        
    if config == None:
        config = read_config()
    if result_table == None:
        result_table = config['DATABASE']['RESULT']
    if queue_table == None:
        queue_table = config['DATABASE']['QUEUE']
    if processed_table == None:
        processed_table = config['DATABASE']['PROCESSED']
    if error_table == None:
        error_table = config['DATABASE']['ERROR']

    s = time.time()
    
    tprint('[·] Downloading and processing data from table...')
    
    tt = TempTable(result_table=result_table,
                   df=df,
                   debug=debug,
                   n_pools=n_pools,
                   n=n,
                   queue_table=queue_table,
                   processed_table=processed_table,
                   error_table=error_table,
                   delete=delete,
                   engine=engine,
                   con=con,
                   rand=rand)
               
    if not tt.df.empty:
        
        t1 = time.time()
        tprint('[+] Done ({} seconds)'.format(round(t1-s,2)))
        tprint('[·] Inserting into main table...')

        tt.update()

        f = time.time()
        
        tprint('[+] Done ({} seconds)'.format(round(f-t1,2)))
        tprint('[+] {}/{} news scraped in {} seconds. ({} s/article)'.format(len(tt.press), n, round(f - s,2), round((f - s)/n, 2)))
        status = 'working'
               
    else:
        # Terminate job when there are no links left.

        tprint('[+] DONE, updated every article.')
        status = 'done'
        
    tt.close_mysql()
    
    return tt.df, status


class TempTable:
    
    def __init__(self,
                 result_table=None,
                 df=None,
                 debug=True,
                 n_pools=15,
                 n=150,
                 queue_table=None,
                 processed_table=None,
                 error_table=None,
                 delete=False,
                 engine=None,
                 con=None,
                 rand=False,
                 config=None):
        
        if df is None:
            df = pd.DataFrame()
        if config == None:
            config = read_config()
        if result_table == None:
            result_table = config['DATABASE']['RESULT']
        if queue_table == None:
            queue_table = config['DATABASE']['QUEUE']
        if processed_table == None:
            processed_table = config['DATABASE']['PROCESSED']
        if error_table == None:
            error_table = config['DATABASE']['ERROR']

        self.result_table = result_table
        self.queue_table = queue_table
        self.processed_table = processed_table
        self.error_table = error_table
        
        self.press = pd.DataFrame()
        self.error = pd.DataFrame()
        self.table_name = None
        self.df = df
        self.debug = debug
        self.engine = engine
        self.con = con
        
        if self.engine==None:
            self.engine = mysql_engine()
        if self.con==None:
            self.con = self.engine.connect()
        
        if self.df.empty:
            self.df = get_full_df(n_pools=n_pools,
                                  n=n,
                                  queue_table=queue_table,
                                  processed_table=processed_table,
                                  delete=delete,
                                  engine=self.engine,
                                  con=self.con,
                                  rand=rand)

        self.divide_df()

    def update(self):

        if self.debug == True:
            self.update_debug()
        else:
            self.update_nodebug()
 

    def update_nodebug(self):
                    
            
        try:
            self.press.to_sql(self.result_table, con = self.con, if_exists='append', index=False)
            try:
                self.error.to_sql(self.error_table, con = self.con, if_exists='append', index=False)

            except Exception as exc:
                tprint('[-] Error updating error TempTable - ', exc)    
                
        except Exception as exc:
            error = f'[-] Error updating {self.result_table} table TempTable - '+str(exc)
            tprint(error[:300])
            try:
                save = self.df
                save['info'] = save['info'].fillna(error[:255])
                save[['original_link', 'borrar', 'info']].to_sql(self.error_table, con = self.con, if_exists='append', index=False)
            except Exception as exc:
                tprint('[-] Error trying to save extracted rows TempTable - ', exc)


    def update_debug(self):
        # Pendiente arreglar (borrar, info, columnas de menos)
            
        try:
            self.create()
            self.insert_table()
            
            try:
                self.error.to_sql(self.error_table, con = self.con, if_exists='append', index=False)

            except Exception as exc:
                tprint('[-] Error updating error TempTable - ', exc)    
                
        except Exception as exc:
            error = f'[-] Error updating {self.result_table} table TempTable - '+str(exc)
            tprint(error[:300])
            try:
                save = self.df
                save['info'] = save['info'].fillna(error[:255])
                save[['original_link', 'borrar', 'info']].to_sql(self.error_table, con = self.con, if_exists='append', index=False)
            except Exception as exc:
                tprint('[-] Error trying to save extracted rows TempTable - ', exc)
            
        self.destroy()


    def create(self):
        
        if self.debug == True:
            self.table_name = self.temp_name()
            self.press.to_sql(self.table_name, con=self.con, index=False)
        
    def destroy(self):
        if self.table_name is not None:
            self.engine.execute(f'drop table if exists {self.table_name}')        
        
    def insert_table(self):
        # Pendientes. arreglos
        insert_query = f'INSERT  IGNORE INTO {self.result_table} (titulo, bajada, contenido, autor, fecha, seccion, original_link, fuente, ano, imagen, tags, link) SELECT titulo, bajada, contenido, autor, fecha, seccion, original_link, fuente, ano, imagen, tags, link FROM {self.table_name}'
        
        self.engine.execute(insert_query)

        
    def divide_df(self):
        self.press = self.df[(self.df.error==0) & (self.df.borrar==0)][['titulo', 'bajada', 'contenido', 'autor', 'fecha', 'seccion', 'original_link', 'fuente', 'ano', 'imagen', 'tags', 'link']]
        self.error = self.df[(self.df.error==1) | (self.df.borrar==1)][['original_link', 'borrar', 'info']]
           
        
    def get_temp_tables(self):
        tables = []

        for t in self.engine.execute('show tables'):
            tables += t.values()
        tables = [t for t in tables if 'temp' in t]

        return tables


    def temp_name(self):
        nums = [t.replace('temp', '') for t in self.get_temp_tables()]

        i = 1
        while str(i) in nums:
            i += 1

        return 'temp'+str(i)
    
    
    def close_mysql(self):
        self.con.close()
        self.engine.dispose()
        self.destroy()


def get_temp_tables(engine=None):
    if engine==None:
        engine = mysql_engine()

    tables = []
    temps = engine.execute('show tables')
    
    for t in temps:
        tables += t.values()
    tables = [t for t in tables if 'temp' in t]
    
    return tables


def flush_temp_tables(engine=None):
    tables = get_temp_tables()
    if engine==None:
        engine = mysql_engine()
    
    for t in tables:
        engine.execute('drop table '+t)

