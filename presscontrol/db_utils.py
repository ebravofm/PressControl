from presscontrol.utils import mysql_engine, tprint
from presscontrol.config import config
from datetime import datetime
import pandas as pd
import random

def init_mysql_db(engine=None):
    close = False
    
    if engine == None:
        engine=mysql_engine()
        close = True
        
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
      original_link varchar(300) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
      id int(11) NOT NULL AUTO_INCREMENT,
      PRIMARY KEY (id),
      UNIQUE KEY no_duplicates (original_link)
    ) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4; '''

    result_query = f'''
    CREATE TABLE IF NOT EXISTS {result} (
      id int(11) NOT NULL AUTO_INCREMENT,
      title varchar(255) DEFAULT NULL,
      description text,
      body text,
      authors varchar(300) DEFAULT NULL,
      date datetime DEFAULT NULL,
      section varchar(120) DEFAULT NULL,
      original_link varchar(300) DEFAULT NULL,
      source varchar(120) DEFAULT NULL,
      year smallint(6) DEFAULT NULL,
      image varchar(300) DEFAULT NULL,
      tags varchar(300) DEFAULT NULL,
      link varchar(300) DEFAULT NULL,
      PRIMARY KEY (id),
      UNIQUE KEY no_duplicates (link)
    ) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4'''

    error_query = f'''
    CREATE TABLE IF NOT EXISTS {error} (
        id int(11) NOT NULL AUTO_INCREMENT,
        original_link VARCHAR(300),
        info VARCHAR(300),
        borrar tinyint(1) DEFAULT 0,
        PRIMARY KEY (id)
    ) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4'''
    
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
        
def reinsert_from_error_to_queue(engine=None, con=None, where=''):
    queue_table = config['TABLES']['QUEUE']
    error_table = config['TABLES']['ERROR']
    
    close = False
    close_ = False
    
    if engine == None:
        engine = mysql_engine()
        con = engine.connect()
        close = True

    if con == None:
        con = engine.connect()
        close_ = True
        
    # Where clause
    if where == '':
        where= input('Where clause for mysql query:\n\t- ')
        print()
        
    # Count and confirm
    tprint('[·] Counting links...')
    count = engine.execute(f'select count(*) from {error_table} where {where}').scalar()
    y = input(f'\nAre you sure you want to reinsert {count} links? (y/n): ')
    print()
    
    if y=='y':
    
        # Get links to be reinserted
        tprint('[·] Getting Links...')
        to_be_reinserted = mysql_query_as_set(f'select original_link from {error_table} where {where};', con=con)
        
        # Reinserting into queue
        tprint('[·] Reinserting into queue table...')
        insert_set(to_be_reinserted, queue_table, 'original_link', engine=engine, con=con)

        # Delete from error
        tprint('[·] Deleting from error table...')
        engine.execute(f'delete from {error_table} where {where}')
        
        count_error = engine.execute(f'select count(*) from {error_table}').scalar()
        tprint(f'[+] Done! {count_error} links left in {error_table} table')

    if close == True:
        con.close()
        engine.dispose()
    if close_ == True:
        con.close()
        
        
def delete_error_where(engine=None, con=None, where=''):
    
    processed_table = config['TABLES']['PROCESSED']
    error_table = config['TABLES']['ERROR']
    
    close = False
    close_ = False
    
    if engine == None:
        engine = mysql_engine()
        con = engine.connect()
        close = True

    if con == None:
        con = engine.connect()
        close_ = True
    # Where clause
    if where == '':
        where= input('Where clause for mysql query:\n\t- ')
        print()
        
    # Count and confirm
    tprint('[·] Counting links...')
    count = engine.execute(f'select count(*) from {error_table} where {where}').scalar()
    y = input(f'\nAre you sure you want to remove {count} links? (y/n): ')
    print()
    
    if y=='y':
    
        # Get links to be removed
        tprint('[·] Getting Links...')
        to_be_removed = mysql_query_as_set(f'select original_link from {error_table} where {where};', con=con)

        # Filtering Processed
        tprint('[·] Filtering processed table...')
        processed = mysql_query_as_set(f'select original_link from {processed_table};', con=con)
        processed = processed - to_be_removed
        
        # Delete from processed and error
        tprint('[·] Deleting from processed and error table...')
        engine.execute(f'delete from {processed_table}')
        engine.execute(f'delete from {error_table} where {where}')
        
        # Reinserting into processed
        tprint('[·] Reinserting into processed table...')

        insert_set(processed, processed_table, 'original_link', engine=engine, con=con)
        count_error = engine.execute(f'select count(*) from {error_table}').scalar()
        tprint(f'[+] Done! {count_error} links left in {error_table} table')

    if close == True:
        con.close()
        engine.dispose()
    if close_ == True:
        con.close()

    
def list_discarded(con=None):
    close = False

    if con == None:
        engine = mysql_engine()
        con = engine.connect()
        close = True
        
    tables = ['PROCESSED', 'RESULT', 'ERROR']
        
    sets = {}
    for table in tables:
        sets[table]= mysql_query_as_set(f"select original_link from {config['TABLES'][table]}")
                
    discarded = sets['PROCESSED'] - sets['RESULT'] - sets['ERROR']
    
        
    if close == True:
        con.close()
        engine.dispose()
        
    return list(discarded)


def mysql_query_as_set(query, con=None):
    '''Gets the first column of a MySQL query as a set'''
    
    if con == None:
        engine = mysql_engine()
        con = engine.connect()

    df = pd.read_sql(query, con=con, chunksize=50000)
    query_set = set()
    for d in df:
        query_set.update(d.iloc[:,0].tolist())
        
    return query_set


def insert_set(iterable, table, column, engine=None, con=None):

    if engine == None and con == None:
        engine = mysql_engine()
        
    if con == None:
        con = engine.connect()
    
    iterable = list(iterable)
    
    df = pd.DataFrame({'original_link': iterable})
    
    df.to_sql('erase',
              con = con,
              if_exists='replace',
              index=False, 
              chunksize=50000)
    engine.execute(f'insert ignore into {table} ({column}) select {column} from erase')
    engine.execute('drop table erase')

        
def recover_discarded(con=None, table=None):
    
    if table == None:
        table = config['TABLES']['ERROR']
        
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
              index=False,
              chunksize=50000)

    
    if close == True:
        con.close()
        engine.dispose()

        
def get_press_rows(n=1, engine=None, con=None, 
                   result=None, 
                   ids=None, source=None):
                    
    queue = config['TABLES']['QUEUE']
    processed = config['TABLES']['PROCESSED']
    result = config['TABLES']['RESULT']
    error = config['TABLES']['ERROR']
    backup = config['TABLES']['BACKUP']
    db = config['MYSQL']['DB']
    
    close_engine = False
    close_con = False
    
    if engine == None:
        engine = mysql_engine()
        close_engine = True
        
    if con == None:
        con = engine.connect()
        close_con = True
        
        
        
    if source == None and ids == None:
        n_rows = engine.execute(f'SELECT Auto_increment FROM information_schema.tables WHERE table_name = "{result}" and table_schema = "{db}";').scalar()
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


def shuffle_table(table=None, engine=None, shuffle=''):
    if table is None:
        table = config['TABLES']['QUEUE']
        
    print()
    if shuffle is True:
        x = 'y'
    elif shuffle is False:
        x = 'n'
    else:
        x = input(f'Shuffle {table}? Recomended to prevent IP being banned (y/n): ')
    print()
    
    if x == 'y':

        if engine == None:
            engine = mysql_engine()

        temp = f'{table}_backup_'+datetime.now().strftime('%d_%m')

        tprint(f'[·] Shuffling table {table} (can take up to 5 mins).')

        engine.execute(f'create table {temp} like {table}')
        engine.execute(f'insert into {temp} (original_link) select original_link from {table} order by rand()')
                
        engine.execute(f'drop table {table}')
        engine.execute(f'rename table {temp} to {table}')
        tprint('[+] Done shuffling.')