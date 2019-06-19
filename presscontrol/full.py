from presscontrol.single import work
from presscontrol.utils import tprint, mysql_engine, len_tables
from presscontrol.db_utils import init_mysql_db, recover_discarded
from presscontrol.config import config
import pandas as pd
import sys

def program(result_table=None, 
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
            rand=False):
    
    if df is None:
        df = pd.DataFrame
        
    if result_table == None:
        result_table = config['TABLES']['RESULT']
    if queue_table == None:
        queue_table = config['TABLES']['QUEUE']
    if processed_table == None:
        processed_table = config['TABLES']['PROCESSED']
    if error_table == None:
        error_table = config['TABLES']['ERROR']
    
    
    # Initialiazing...
    if engine == None:
        engine = mysql_engine()
        con = engine.connect()
    if con == None:
        con = engine.connect()

    print()
    tprint('[·] Initializing...')
    status = 'working'
    init_mysql_db(engine=engine)
    recover_discarded(con=con)
    queue = len_tables('queue')['queue']
    tprint('[+] Done.')
    print()
    
    con.close()
    engine.dispose()

    
    
    i = 1
    while queue != 0:
        
        engine = mysql_engine()
        con = engine.connect()
        
        try:
            result, status = work(result_table=result_table, 
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
            
            if status == 'done':
                tprint('[+] DONE!')
                input('\n(ENTER)')
                sys.exit()

        except KeyboardInterrupt:
            sys.exit()
            
        except Exception as exc:
            tprint('[-] Error General - ', exc)
        print()
                
        con.close()
        engine.dispose()
        
        if i%100 == 0:
            
            queue = len_tables('queue')['queue']
            tprint('[+] {} left in queue.'.format(queue))
            print()
            
        i += 1
        
    tprint('[+] DONE!')
        
if __name__ == "__main__":
    program()



