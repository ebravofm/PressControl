from PressControl.single import work, flush_temp_tables
from PressControl.utils import tprint, mysql_engine, len_tables
from PressControl.db_utils import init_mysql_db, recover_discarded

def program():
    
    # Initialiazing...
    
    print()
    tprint('[·] Initializing...')
    status = 'working'
    init_mysql_db()
    recover_discarded()
    queue = len_tables('queue')['queue']
    tprint('[+] Done.')
    print()
    
    
    i = 1
    while queue != 0:
        
        engine = mysql_engine()
        con = engine.connect()
        
        try:
            df, status = work(n=150, engine=engine, con=con, debug=False, delete=True)
        except Exception as exc:
            tprint('[-] Error General - ', exc)
        print()
                
        con.close()
        engine.dispose()
        flush_temp_tables()
        
        if i%100 == 0:
            
            queue = len_tables('queue')['queue']
            tprint('[+] {} left in queue.'.format(queue))
            print()
            
        i += 1
        
    tprint('[+] DONE!')
        
if __name__ == "__main__":
    program()



