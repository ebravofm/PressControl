from presscontrol.utils import tprint, mysql_engine
from presscontrol.db_utils import shuffle_queue
from datetime import datetime, timedelta
import presscontrol.GetOldTweets as got
from presscontrol.config import config
import pandas as pd
import calendar
import os

def tweet2url(tweets=None, user=None, days=0, months=0, years=0, 
              monthly=False, yearly=False, since='',until=''):
    '''Get URLs from a list of tweets (GetOldTweets class) or perform tweet scraping by specifying a user and dates.'''
    
    urls = []

    if tweets == None:
        tweets = scrape_tweets(user=user, days=days, months=months, 
                               years=years, monthly=monthly,
                               yearly=yearly, since=since, until=until)
    for tweet in tweets:
        urls += tweet.urls.split(',')
        
    urls = [u for u in urls if u!='']
    urls = list(set(urls))
            
    return urls


def scrape_tweets(user, days=0, months=0, years=0, monthly=False, 
                  yearly=False, since='', until=''):

    tweets = []
    summary = Summary()
    
    if until == '':
        until = datetime.today()+timedelta(1)
    else:
        until = datetime.strptime(until, '%Y-%m-%d')
    
    
    if since == '':
        d = int(days + months*31 + years*365)
        since = until - timedelta(d)
    else:
        since = datetime.strptime(since, '%Y-%m-%d')

    since_ = since
    
    while since_ < until:
        until_ = next_date(since_) if next_date(since_) < until else until
        
        if since_.day == until_.day:
            tprint(f'[·] Getting tweets from {calendar.month_name[since_.month]} {since_.year}')
        else:
            tprint(f'[·] Getting tweets from {dt2str(since_)} to {dt2str(until_)}')

        tweetCriteria = got.manager.TweetCriteria().setUsername(user).setSince(dt2str(since_)).setUntil(dt2str(until_))
        tweets_ = got.manager.TweetManager.getTweets(tweetCriteria)
        
        tprint(f'[+] Done ({len(tweets_)} tweets).')
        summary.add_entry(user, since_.year, since_.month, len(tweets_))
        tweets += tweets_
        since_ = until_
        
    print()
    tprint(f'[+] DONE ({len(tweets)} tweets)')
        
    return tweets, summary


def links2db(urls, con=None, display='', save='', update=''):
    
    backup = config['TABLES']['BACKUP']
    queue = config['TABLES']['QUEUE']

    if con == None:
        engine = mysql_engine()
        con = engine.connect()

    engine.execute('drop table if exists erase')
        
    df = pd.DataFrame(urls, columns=['original_link'])
    
    if display == False:
        display = 'n'
    elif display == True:
        display = 'y'
    else:
        display = input(f'Display {len(df)} links? (y/n): ')
    
    if display == 'y':
        print()
        big_str = '\n'.join(urls)
        os.system(f"echo '{big_str}' | more -10")
                    

    if save == False:
        save = 'n'
    if save == True:
        save = 'y'
    else:
        save = input(f'Save {len(df)} links to csv file? (y/n): ')

    if save == 'y':
        print()
        file_name = input('File name (implicit .csv): ~/presscontrol/')
        df.to_csv(f"{os.environ['HOME']}/presscontrol/{file_name}.csv", index=False, encoding='utf-8-sig', header=False)
        print(f'Articles Saved to ~/presscontrol/{file_name}.csv')
        print()
        
    if update == False:
        update = 'n'
    elif update == True:
        update = 'y'
    else:
        update = input(f'Add {len(df)} links to database (y/n): ')

    if update == 'y':
        print()
        df.to_sql('erase', con=con, index=False, if_exists='append')

        engine.execute(f'insert ignore into {backup} (original_link) select original_link from erase')
        engine.execute(f'insert ignore into {queue} (original_link) select original_link from erase')

        engine.execute('drop table if exists erase')
        
        print('Success.')
        
        shuffle_queue(engine=engine)
        

class Summary:
    def __init__(self):
        self.users = {}
        
    def add_entry(self, user, year, month, number):
        if user not in self.users.keys():
            self.users[user] = {}
        if year not in self.users[user].keys():
            self.users[user][year] = {}
        
        if type(month) == int:
            month = calendar.month_abbr[month]
            
        self.users[user][year][month] = number
        
    def print(self):
        pass

    
def next_date(sourcedate):
    
    if sourcedate.day == 1:
    
        month = sourcedate.month
        year = sourcedate.year + month // 12
        month = month % 12 + 1
        day = min(sourcedate.day, calendar.monthrange(year,month)[1])

        return datetime(year, month, day)
    
    else: 
        month = sourcedate.month
        year = sourcedate.year + month // 12
        month = month % 12
        day = calendar.monthrange(year,month)[1]

        return datetime(year, month, day) + timedelta(1)
        
def dt2str(dt):    
    return dt.strftime('%Y-%m-%d')

def str2dt(string_date):
    return datetime.strptime(string_date, '%Y-%m-%d')