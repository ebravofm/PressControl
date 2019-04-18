#!/usr/bin/python3

import os
import inspect
from functools import partial
from PressControl.db_utils import get_press_rows
from PressControl.utils import read_config, add_cookies
from PressControl.scrape import tweet2url, links2db, shuffle_queue
from PressControl.full import program
import textwrap


def cli(work=False):
    if work == True:
        program(delete=True)
    else:
        Home()
    

# =======================
#       MAIN MENU
# =======================

def Home():
    title='Press Control'
    options = [
        ['Get Articles', get_article],
        ['Scrape New Links', scrape_new_links],
        ['Add links', TBA],
        ['SQL', TBA],
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
    if n == None:
        while type(n)!=int:
            try:
                print()
                n = int(input('How many: '))
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
                
        elif c=='2':
            file_name = input('File name (implicit .csv): temp/')
            rows.to_csv('temp/{}.csv'.format(file_name), index=False, sep='|', encoding='utf-8-sig')
            print('Articles Saved to temp/'+file_name+'.csv')
            print()
        
            input('(ENTER)')
    Home()
    
    
# =======================
#    SCRAPE NEW LINKS
# =======================

def scrape_new_links():
    title='Scrape New Links'
    options = [
        ['Scrape Twitter', scrape_twitter],
        ['Scrape SiteMap', TBA],
        ['Shuffle Queue', shuffle]]    
    UI(title=title, options=options, home=False)

    
def scrape_twitter():
    username = input('\nUsername: ')
    date = input('\n[1] Set date range\n[2] Days/months/years old\n\n>>> ')
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
        
    os.system('clear')
    print((' Tweets: '+username+' ').center(62, '+'))
    print()
    urls = tweet2url(username=username, days=days, months=months, 
                     years=years, since=since, until=until)
    
    links2db(urls)
    Home()

    
def shuffle():
    suffle_queue(engine=None)
    input('(ENTER)')
    Home()
    

# =======================
#      CONFIGURATION
# =======================

def configuration():
    title='Configuration'
    options = [
        ['Allowed domains', TBA],
        ['Add Cookies', add_cookie]]    
    UI(title=title, options=options, home=True)
    

def add_cookie():
    add_cookies()
    Home()
    
    
# Pending
def allowed_domains():
    title='Allowed Domains'
    
    description = textwrap.fill('Domains allowed to be stored on database. Currently allowed domains:', 30)+'\n\n    '+'\n    '.join(read_config()['sources'])
    
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
        d_options['0'] = ['Home', home]
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
    
