from PressControl.utils import mysql_engine, read_config
import pandas as pd
import random

def init_mysql_db(engine=None, config=None):
    close = False
    
    if engine == None:
        engine=mysql_engine()
        close = True
    if config == None:
        config = read_config()
        
    queue = config['TABLES']['QUEUE']
    processed = config['TABLES']['PROCESSED']
    result = config['TABLES']['RESULT']
    error = config['TABLES']['ERROR']
    backup = config['TABLES']['BACKUP']

    processed_query = f'''
    CREATE TABLE IF NOT EXISTS {processed} (
        original_link VARCHAR(300)
    ) CHARSET=utf8mb4; '''

    queue_query = f'''
    CREATE TABLE IF NOT EXISTS {queue} (
        id INT(11) NOT NULL Auto_increment,
        original_link VARCHAR(300),

        PRIMARY KEY (id)
    ) CHARSET=utf8mb4; '''

    result_query = f'''
    CREATE TABLE IF NOT EXISTS {result} (
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

    error_query = f'''
    CREATE TABLE IF NOT EXISTS {error} (
        original_link VARCHAR(300),
        info VARCHAR(300),
        borrar tinyint(1) DEFAULT 0
    ) CHARSET=utf8mb4; '''
    
    engine.execute(processed_query)
    engine.execute(queue_query)
    engine.execute(result_query)
    engine.execute(error_query)
    
    # No duplicates
    
    result_no_dup = f'alter table {result} add constraint no_duplicates unique key(link)'
    queue_no_dup = f"alter table {queue} add constraint no_duplicates unique key(original_link)"
    backup_no_dup = f"alter table {backup} add constraint no_duplicates unique key(original_link)"
    
    try:
        engine.execute(result_no_dup)
    except: pass    
    try:
        engine.execute(queue_no_dup)
    except: pass    
    try:
        engine.execute(backup_no_dup)
    except: pass

    if close == True:
        engine.dispose()
    
    
def list_discarded(con=None, config=None):
    close = False
    if config == None:
        config = read_config()
    if con == None:
        engine = mysql_engine()
        con = engine.connect()
        close = True

    queue = config['TABLES']['QUEUE']
    processed = config['TABLES']['PROCESSED']
    result = config['TABLES']['RESULT']
    error = config['TABLES']['ERROR']
    backup = config['TABLES']['BACKUP']

    processed_list = []
    result_list = []
    error_list = []
    
    ps = pd.read_sql(f"select original_link from {processed}", con=con, chunksize=50000)
    for p in ps:
        processed_list += p.original_link.tolist()
        
    rs = pd.read_sql(f"select original_link from {result}", con=con, chunksize=50000)
    for r in rs:
        result_list += r.original_link.tolist()
        
    es = pd.read_sql(f"select original_link from {error}", con=con, chunksize=50000)
    for e in es:
        error_list += e.original_link.tolist()
        
    discarded = set(processed_list) - set(result_list) - set(error_list)
        
    if close == True:
        con.close()
        engine.dispose()
        
    return list(discarded)


def recover_discarded(con=None, table=None):
    
    if table == None:
        table = read_config()['TABLES']['ERROR']
        
    close = False
    
    if con == None:
        engine = mysql_engine()
        con = engine.connect()
        close = True

        
    d = list_discarded(con=con)
    df = pd.DataFrame({'original_link': d, 
                       'info': ['Discarded by insert ignore.']*len(d), 
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
                   ids=None, source=None, config=None):
    
    if result_table==None:
        result_table = config['TABLES']['RESULT']
    
    if config == None:
        config = read_config()
        
    queue = config['TABLES']['QUEUE']
    processed = config['TABLES']['PROCESSED']
    result = config['TABLES']['RESULT']
    error = config['TABLES']['ERROR']
    backup = config['TABLES']['BACKUP']

    close_engine = False
    close_con = False
    
    if engine == None:
        engine = mysql_engine()
        close_engine = True
        
    if con == None:
        con = engine.connect()
        close_con = True
        
        
        
    if source == None and ids == None:
        n_rows = engine.execute(f'SELECT Auto_increment FROM information_schema.tables WHERE table_name = "{result}";').scalar()
        ids = random.sample(range(1, n_rows), n)

    query = f'select * from {result} where '
    for id in ids:
        query += 'id='+str(id)+' or '
    query = query.strip(' or ')

    
    rows = pd.read_sql(query, con)

        
    if close_con == True:
        con.close()
    if close_engine == True:
        engine.dispose()

    
    return rows