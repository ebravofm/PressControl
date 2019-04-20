#!/usr/bin/python3

from presscontrol.db_utils import get_press_rows, shuffle_queue
from presscontrol.utils import add_cookies
from presscontrol.scrape import tweet2url, links2db
from presscontrol.full import program
from presscontrol.config import config
from functools import partial
import inspect
import textwrap
import os


def cli(work=False):
    if work == True:
        program(delete=True)
    else:
        try:
            Home()
        except KeyboardInterrupt:
            Home()

# =======================
#       MAIN MENU
# =======================

def Home():
    title='Press Control'
    options = [
        ['Get Articles', get_article],
        ['Work', work_UI],
        ['Twitter', twitter],
        ['Manage Database', manage_db],
        ['Configuration', configuration]]    
    UI(title=title, options=options, home=True)
    
    
# =======================
#      GET ARTICLES
# =======================

def get_article():
    title='Get Articles'
    options = [
        ['Single article', partial(articles, 1)],
        ['Multiple articles', articles]]
    
    UI(title=title, options=options, home=False)


def articles(n=None, engine=None, con=None):
    try:
        if n == None:
            while type(n)!=int:
                try:
                    print()
                    n = int(input('How many: '))
                except KeyboardInterrupt:
                    Home()
                except:
                    pass

        rows = get_press_rows(n=n, engine=engine, con=con)

        if n ==1:
            print_article(rows.iloc[0])
            input('(ENTER)')


        else:
            print()
            print('[1] Print')
            print('[2] Save to .csv')
            print()
            print('[0] Cancel')
            print()

            c = input('>>> ')
            if c=='1':
                for n, r in rows.iterrows():
                    print()
                    print('  {}  '.format(n+1).center(80, '*'))
                    print()
                    print_article(r)
                    print()
                    input('(ENTER {})'.format(n+1))

                print()    
                save = input('Save to csv? (y/n): ')
                if save == 'y':

                    file_name = input('File name (implicit .csv): ~/presscontrol/')
                    rows.to_csv(f"{os.environ['HOME']}/presscontrol/{file_name}.csv", index=False, sep='|', encoding='utf-8-sig')
                    print(f'Articles Saved to ~/presscontrol/{file_name}.csv')
                    print()
                    input('(ENTER)')

            elif c=='2':
                file_name = input('File name (implicit .csv): ~/presscontrol/')
                rows.to_csv(f"{os.environ['HOME']}/presscontrol/{file_name}.csv", index=False, sep='|', encoding='utf-8-sig')
                print(f'Articles Saved to ~/presscontrol/{file_name}.csv')
                print()

                input('(ENTER)')
    except KeyboardInterrupt:
        pass
    Home()
    
# =======================
#          WORK
# =======================

def work_UI():
    
    result = config['TABLES']['RESULT']
    queue = config['TABLES']['QUEUE']
    
    title='Work'
    description = textwrap.fill(f'This program executes a script to fill out the results table ({result}) and feeds from the queue table ({queue}). The program takes about 0.4 secods per article so it could take a while. It is recommended to run this program on a multiplexer to keep the session active.', 60)
    options = [['Start Program', partial(program, delete=True)]]  
    
    UI(title=title, description=description, options=options, home=False)

# =======================
#        TWITTER
# =======================
    
def twitter():
    title='Twitter'
    options = [
        ['Scrape Twitter Accounts', scrape_twitter],
        ['Program Twitter Task', TBA]]    
    UI(title=title, options=options, home=False)

    
def scrape_twitter():
    check = True
    tasks = []
    counter = 1
    
    while check:
        try:
            os.system('clear')
            print(f'Task {counter}'.center(30, '+'))
            print('\nCtrl+C to begin scraping.')
            counter += 1
            
            username = input('\nUsername: ')
            date = input('\n[1] Set date range'+
                         '\n[2] Days/months/years old\n'+
                         '\n>>> ')
            print()
            since = ''
            until = ''
            years = 0
            months = 0
            days = 0

            if date == '1':
                since = input('Since (YYYY-MM-DD): ')
                until = input('Until (YYYY-MM-DD): ')
            elif date == '2':
                try:
                    years = int(input('Years: '))
                except: pass
                try: 
                    months = int(input('Months: '))
                except: pass
                try:
                    days = int(input('Days: '))
                except: pass
            
            tasks += [{'username': username,
                       'since': since,
                       'until': until,
                       'years': years,
                       'months': months,
                       'days': days}]

            os.system('clear')
        
        except KeyboardInterrupt:
            os.system('clear')
            print(f'Number of tasks: {len(tasks)}')
            print()
            check = False
            b = input('Begin Scraping (y/n): ')
            print()
            
    if len(tasks) > 1:
        urls = []
        opt = input('\n[1] Display Links '+
                     '\n[2] Save links to .csv'+
                     '\n[3] Add links to database\n'+
                     '\n>>> ')
        opts = {'1': False, '2': False, '3': False}
        opts[opt] = True
        
        for task in tasks:

            print((f" Tweets: {task['username']} ").center(62, '+'))
            print()

            url = tweet2url(username=task['username'], 
                             days=task['days'], 
                             months=task['months'], 
                             years=task['years'], 
                             since=task['since'], 
                             until=task['until'])
            urls += url

        links2db(urls, display=opts['1'], save=opts['2'], update=opts['3'])
    
    else:
        for task in tasks:

            print((f" Tweets: {task['username']} ").center(62, '+'))
            print()

            urls = tweet2url(username=task['username'], 
                             days=task['days'], 
                             months=task['months'], 
                             years=task['years'], 
                             since=task['since'], 
                             until=task['until'])

        links2db(urls)

    Home()

    
def shuffle():
    shuffle_queue(engine=None)
    input('(ENTER)')
    Home()
    

# =======================
#    MANAGE DATABASE
# =======================

def manage_db():
    title='Manage Database'
    options = [
        ['Shuffle Queue', shuffle]]    
    UI(title=title, options=options, home=False)
    

# =======================
#      CONFIGURATION
# =======================

def configuration():
    title='Configuration'
    options = [
        ['Allowed domains', TBA],
        ['Add Cookies', add_cookie]]    
    UI(title=title, options=options, home=False)
    

def add_cookie():
    add_cookies()
    Home()
    
    
# Pending
def allowed_domains():
    title='Allowed Domains'
    
    description = textwrap.fill('Domains allowed to be stored on database. Currently allowed domains:', 30)+'\n\n    '+'\n    '.join(config['SOURCES'])
    
    options = [
        ['Add domain', TBA],
        ['Delete domain', TBA]]    
    
    UI(title=title, description=description, options=options, home=False)


# =======================
#         UTILS
# =======================    
    
    
def UI(title='', description='', options=[], home=False):
    os.system('clear')
    
    
    # Print Title
    if title!='':
        print('='*30, sep='')
        print(title.center(30,' '))
        print('='*30, '\n')
        
    if description!='':
        print('\n', description, '\n', sep='')
        
        
    # Print Options
    d_options = {}
    print('Options:\n')
    
    for n, opt in enumerate(options):
        n = str(n+1)
        
        print('[{}] '.format(n) + opt[0])
        d_options[n] = opt
    print()
    
    if home == True:
        d_options['0'] = ['Exit', exit]
        print('[0] Exit\n')

    if home == False:
        d_options['0'] = ['Home', Home]
        print('[0] Home\n')

        
    # Prompt
    c = input('>>> ')
    while c not in d_options.keys():
        print(c, 'is not a valid option.\n')
        c = input('>>> ')
       
    
    # Action
    d_options[c][1]()

    
def TBA():
    UI('TBA')


def exit():
    os.system('clear')


def print_article(r):
    print()
    try: print('URL: '+r.link)
    except: pass
    
    try: print('IMAGEN: '+r.imagen)
    except: pass
    
    try: print('TAGS: '+r.tags)
    except: pass
    print()
    
    try: print(r.fecha.rjust(80, ' '))
    except: pass
    
    try: print(r.fuente, '(id: {})'.format(r.id))
    except: pass
    
    try:
        print('='*80)
        title = textwrap.fill(r.titulo, 80).split('\n')
        for t in title:
            print(t.center(80, ' '))
        print('='*80)
    except: pass
    
    try: print('Author: '+r.autor)
    except: pass
    
    try: 
        print('Section: '+r.seccion)
        print()
    except: pass
    
    try:
        print(textwrap.fill(r.bajada, 80))
        print()
    except: pass
    
    try: 
        print(textwrap.fill(r.contenido, 80))
        print()
    except: pass
    
