'''
Use this file to add your own scraping tasks.
Scraping tasks must be functions which parameters (such as date ranges, websites, etc) are entered by using user input. Output must be a list of links. This links are then added the queue table on the mysql database.
If you want the tasks to be scheduled by cron, make sure they dont require user input at all.

Example:

def scrape_nytimes():
    
    
    return links
    
'''
from selenium import webdriver
from pyvirtualdisplay import Display
from bs4 import BeautifulSoup as bs
from time import sleep
import sys
import os
import datetime


def browser(func):
    def browser_wrapper(*args, **kwargs):
        
        options = webdriver.ChromeOptions()

        if sys.platform == 'linux':
            display = Display(visible=0, size=(800, 600))
            display.start()
            options.add_argument(f"download.default_directory={os.getcwd()}")
            options.add_argument('--no-sandbox')

        d = webdriver.Chrome(options=options)

        print('[+] Logged in.')

        try:
            result = func(d, *args, **kwargs)
            
        except Exception as exc:
            d.close()
            if sys.platform == 'linux':
                display.stop()
            raise RuntimeError(str(exc))


        d.close()
        if sys.platform == 'linux':
            display.stop()
        print('[+] Logged Out.')

        return result
    
    return browser_wrapper


@browser
def emol_scrape(d, from_date='', days='', interactive=False):
    
    if interactive:
        from_date = input('Scrape from date: (dd/mm)')
        if from_date=='':
            try:
                days = int(input('Numbre of days to scrape from: '))
            except:
                pass
    # Setting parameters.
    if days == '': days = 3

    if from_date=='':
        now = datetime.datetime.now()
        delta = datetime.timedelta(days)
        from_date = (now-delta).strftime("%d/%m")

    print(from_date)
    links = []

    # Firing up chromedriver.
    #d = webdriver.Chrome()

    d.get('https://www.emol.com/sitemap/noticias/2019/list')
    
    # Looping over pages.
    i = 0
    check = True

    while check:
        print(f'[·] Page {i}...')
        i += 1
        soup = bs(d.page_source, 'lxml')

        news_list = soup.find('ul', {'id': 'listNews'})
        articles = news_list.find_all('li')

        for art in articles:
            date = art.find('span').text.split('|')[0].strip().split()[0]
            link = art.find('a', {'id': 'LinkNoticia'})['href']
            link = 'www.emol.com' + link.split('www.emol.com')[1]

            if date != from_date:
                links.append(link)

            else:
                print(f'[+] Reached Limit {from_date}')
                check=False
                break

        d.execute_script(f"javascript:SelectPage({i});")
        sleep(1)

    print(f'[+] Session closed. Scraped {len(links)} links.')
    
    return links



