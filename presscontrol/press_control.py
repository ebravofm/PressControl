#!/usr/bin/python3

from presscontrol.db_utils import get_press_rows, shuffle_table
from presscontrol.utils import add_cookies, center_xcols
from presscontrol.scrape import tweet2url, links2db, Summary
from presscontrol.full import program
from presscontrol.config import config
from presscontrol.cron import show_pc_tasks, daily_scrape_twitter
from functools import partial
import inspect
import textwrap
import os


def cli(work=False, scrape=False, display='', save='', update='', 
        file_name=None, print_sum='', dump_sum='', interactive=False, 
        username='', since='', until='', years=0, months=0, days=0,
        shuffle=False):
    
    if work == True:
        program(delete=True)
        
    elif scrape==True:
        if username == '': raise
        scrape_twitter(display=display,
                       save=save,
                       update=update,
                       file_name=file_name,
                       print_sum=print_sum,
                       dump_sum=dump_sum,
                       interactive=interactive,
                       username=username,
                       since=since,
                       until=until,
                       years=years,
                       months=years,
                       days=days,
                       shuffle=shuffle)
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
        ['Tasks', tasks],
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

    
def scrape_twitter(display='', save='', update='', file_name=None, 
                   print_sum='', dump_sum='', interactive=True, 
                   username='', since='', until='', years=0, 
                   months=0, days=0, shuffle=False):
    
    check = True
    summary = Summary()
    counter = 1
    
    if interactive is True:
        tasks = []
        while check:
            try:
                os.system('clear')
                print(f' Task {counter} '.center(30, '+'))
                print('\nCtrl+C to begin scraping.')
                counter += 1

                username = input('\nUsername: ')
                date = input('\n[1] Set date range'+
                             '\n[2] Days/months/years old\n'+
                             '\n>>> ')
                print()

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
                check = False
            
    else:
        tasks = [{'username': username,
                  'since': since,
                  'until': until,
                  'years': years,
                  'months': months,
                  'days': days}]
        
    if len(tasks) > 0:
        urls = []
        if display=='' and save=='' and update=='':
            opt = input('\n[1] Display Links '+
                        '\n[2] Save links to .csv'+
                        '\n[3] Add links to database\n'+
                        '\nSelect options separated by space: ')

            opt = [int(o) for o in opt.split()]
            if 1 in opt: display = True
            if 2 in opt: save = True
            if 3 in opt: update = True    
                
        if save is True and file_name is None:
            file_name = input('File name (implicit .csv): ~/presscontrol/')

        for task in tasks:
            print()
            print((f" Tweets: {task['username']} ").center(62, '+'))
            print()

            url, summary_ = tweet2url(user=task['username'], 
                             days=task['days'], 
                             months=task['months'], 
                             years=task['years'], 
                             since=task['since'], 
                             until=task['until'])
            urls += url
            summary.add_dict(user=task['username'],
                             d=summary_.sum)

        links2db(urls, display=display, save=save, update=update, file_name=file_name, shuffle=shuffle)
    
    if print_sum == '':
        print_sum = input('Print Summary? (y/n): ')
    if print_sum=='y' or print_sum is True:
        summary.print()
        
    if dump_sum == '':
        print()
        dump_sum = input('Dump Summary? (y/n): ')
    if dump_sum=='y' or dump_sum is True:
        summary.dump_csv()
        
    if interactive is True:
        input('(ENTER)')

        Home()

    
def shuffle_fn():
    shuffle_table(engine=None)
    input('(ENTER)')
    Home()
    

# =======================
#        TASKS
# =======================

def tasks():
    title='Tasks'
    options = [
        ['Show PressControl Tasks', show_pc_tasks_],
        ['Program Twitter Scraping', program_tw_task]]
    
    UI(title=title, options=options, home=False)

def show_pc_tasks_():
    title='PressControl Tasks'
    options = []
    
    tasks = show_pc_tasks()
    
    if len(tasks)>0:
        description = ''
        for n, task in enumerate(tasks):
            description += f'({n}) {task}\n'
    else:
        description = 'No tasks'
        
    UI(title=title, description=description, options=options, home=False)
    
def program_tw_task():
    print()
    usernames = input('Enter Usernames separated by space.\n>>> ').split()
    daily_scrape_twitter(usernames)
    
    input('(ENTER)')
    Home()


# =======================
#    MANAGE DATABASE
# =======================

def manage_db():
    title='Manage Database'
    options = [
        ['Shuffle Queue', shuffle_fn]]    
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
    
    try: print('IMAGEN: '+r.image)
    except: pass
    
    try: print('TAGS: '+r.tags)
    except: pass
    print()
    
    try: print(r.date.rjust(80, ' '))
    except: pass
    
    try: print(r.source, '(id: {})'.format(r.id))
    except: pass
    
    try:
        print('='*80)
        title = textwrap.fill(r.title, 80).split('\n')
        for t in title:
            print(t.center(80, ' '))
        print('='*80)
    except: pass
    
    try: print('Author: '+r.authors)
    except: pass
    
    try: 
        print('Section: '+r.section)
        print()
    except: pass
    
    try:
        print(textwrap.fill(r.description, 80))
        print()
    except: pass
    
    try: 
        print(textwrap.fill(r.body, 80))
        print()
    except: pass
    
