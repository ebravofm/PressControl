from presscontrol.config import config
from sqlalchemy import create_engine
import pandas as pd
import datetime
import shutil
import os


def mysql_engine():
        
    host = config['MYSQL']['HOST']
    port = config['MYSQL']['PORT']
    user = config['MYSQL']['USER']
    passwd = config['MYSQL']['PASSWD']
    db = config['MYSQL']['DB']

    connector = f'mysql+mysqlconnector://{user}:{passwd}@{host}:{port}/{db}?charset=utf8mb4'
    engine = create_engine(connector, echo=False)

    return engine


def tprint(*args, important=True):

    debug = config['CONFIG']['DEBUG']
        
    if important == True:
        stamp = '[{:%d/%m-%H:%M}]'.format(datetime.datetime.now())
        print(stamp, *args)

    elif important == False and debug:
        print('                  ', *args)
        

def show_tables(engine=None):
    if engine == None:
        engine = mysql_engine()
        
    tables = engine.execute('show tables')
    tables = [t.values()[0] for t in tables]
    
    return tables


def show_table_columns(table, engine=None):
    if engine == None:
        engine = mysql_engine()

    columns = engine.execute('desc '+table)
    columns = [t[0] for t in columns]
    
    return columns


def len_tables(tables=None, engine=None, con=None):
    close = False

    if engine == None:
        engine = mysql_engine()

    if con==None:
        con = engine.connect()
        close = True
        
    
    if type(tables) == str:
        tables = [tables]
        
    elif tables == None:
        tables = show_tables(engine)
        
    d = {}

    for table in tables:
        col = show_table_columns(table, engine)[0]

        dfs = pd.read_sql('select '+col+' from '+table, con=con, chunksize=50000)

        d[table] = 0
        for df in dfs:
            d[table] += len(df)
    return d

    if close == True:
        con.close()
        engine.dispose()


def read_cookies():
    cookies = {}
    path = f"{os.environ['HOME']}/presscontrol/cookies/"
    if os.path.exists(path):
        
        cookies_path = [path+cookie for cookie in os.listdir(path) if 'pkl' in cookie]
        for cookie in cookies_path:
            cookies[cookie.split('/')[-1].split('.')[0]] = pd.read_pickle(cookie)
            
    return cookies
 
    
def add_cookies(file_path=None):   
    path = f"{os.environ['HOME']}/presscontrol/cookies/"
    
    try:
        if file_path == None:
            file_path = input('Cookie file path: ')
        if not os.path.exists(path):
            os.makedirs(path)
        shutil.copyfile(file_path, path+file_path.split('/')[-1])
        
    except FileNotFoundError:
        print()
        print('File Not Found')
        print()
        input('(ENTER)')
    
def create_home_dir():
    path = f"{os.environ['HOME']}/presscontrol/"
    if not os.path.exists(path):
        os.makedirs(path)
        

# Pending
'''def write_config(field, value, append):
    with open('config.conf', 'r') as input_file, open('config.conf', 'w') as output_file:
        for line in input_file:
            if field in line:
                output_file.write(line.split('=')[0])
            else:
                output_file.write(line)   ''' 