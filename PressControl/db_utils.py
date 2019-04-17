from PressControl.utils import mysql_engine, read_config
import pandas as pd
import random

def init_mysql_db(engine=None):
    close = False
    
    if engine == None:
        engine=mysql_engine()
        close = True
    
    
    c = read_config()

    processed_query = '''
    CREATE TABLE IF NOT EXISTS '''+c['processed_table']+''' (
        original_link VARCHAR(300)
    ) CHARSET=utf8mb4; '''

    queue_query = '''
    CREATE TABLE IF NOT EXISTS '''+c['queue_table']+''' (
        original_link VARCHAR(300)
    ) CHARSET=utf8mb4; '''

    result_query = '''
    CREATE TABLE IF NOT EXISTS '''+c['result_table']+''' (
        id INT(11) NOT NULL Auto_increment,
        titulo VARCHAR(255),
        bajada TEXT,
        contenido TEXT,
        autor VARCHAR(300),
        fecha VARCHAR(120),
        seccion VARCHAR(120),
        original_link VARCHAR(300),
        fuente VARCHAR(120),
        ano SMALLINT(6),
        imagen VARCHAR(300),
        tags VARCHAR(300),
        link VARCHAR(300),

        PRIMARY KEY (id)
    ) CHARSET=utf8mb4 Auto_increment=1; '''

    error_query = '''
    CREATE TABLE IF NOT EXISTS '''+c['error_table']+''' (
        original_link VARCHAR(255),
        info VARCHAR(300),
        borrar tinyint(1) DEFAULT 0
    ) CHARSET=utf8mb4; '''
    
    engine.execute(processed_query)
    engine.execute(queue_query)
    engine.execute(result_query)
    engine.execute(error_query)
    
    # No duplicates
    
    #result_no_dup = 'alter table '+c['result_table']+' add constraint no_duplicates unique key(link)'
    queue_no_dup = 'alter table '+c['queue_table']+' add constraint no_duplicates unique key(original_link)'
    backup_no_dup = 'alter table '+c['backup_table']+' add constraint no_duplicates unique key(original_link)'
    
    '''try:
        engine.execute(result_no_dup)
    except: pass'''    
    try:
        engine.execute(queue_no_dup)
    except: pass    
    try:
        engine.execute(backup_no_dup)
    except: pass

    if close == True:
        engine.dispose()
    
    
def list_discarded(con=None):
    close = False
    c = read_config()
    
    if con == None:
        engine = mysql_engine()
        con = engine.connect()
        close = True
        
        

    processed = []
    result = []
    error = []
    
    ps = pd.read_sql('select original_link from '+c['processed_table'], con=con, chunksize=50000)
    rs = pd.read_sql('select original_link from '+c['result_table'], con=con, chunksize=50000)
    es = pd.read_sql('select original_link from '+c['error_table'], con=con, chunksize=50000)
    
    for p in ps:
        processed += p.original_link.tolist()
    for r in rs:
        result += r.original_link.tolist()
    for e in es:
        error += e.original_link.tolist()
        
    discarded = set(processed) - set(result) - set(error)
        
        
        
    if close == True:
        con.close()
        engine.dispose()
        
    
    return list(discarded)


def recover_discarded(con=None, table=None):
    
    if table == None:
        table = read_config()['error_table']
        
    close = False
    
    if con == None:
        engine = mysql_engine()
        con = engine.connect()
        close = True

        
    d = list_discarded(con=con)
    df = pd.DataFrame({'original_link': d, 
                       'info': ['Discarded by pandas.']*len(d), 
                       'borrar': [0]*len(d)})
    df.to_sql(table,
              con = con,
              if_exists='append',
              index=False)

    
    if close == True:
        con.close()
        engine.dispose()

        
def get_press_rows(n=1, engine=None, con=None, 
                   result_table=None, 
                   ids=None, source=None):
    
    if result_table==None:
        result_table = read_config()['result_table']
    
    c = read_config()
    close_engine = False
    close_con = False
    
    if engine == None:
        engine = mysql_engine()
        close_engine = True
        
    if con == None:
        con = engine.connect()
        close_con = True
        
        
        
    if source == None and ids == None:
        n_rows = engine.execute('SELECT Auto_increment FROM information_schema.tables WHERE table_name = "'+c['result_table']+'";').scalar()
        ids = random.sample(range(1, n_rows), n)

    query = 'select * from '+c['result_table']+' where '
    for id in ids:
        query += 'id='+str(id)+' or '
    query = query.strip(' or ')

    
    rows = pd.read_sql(query, con)

        
    if close_con == True:
        con.close()
    if close_engine == True:
        engine.dispose()

    
    return rows
        
    
