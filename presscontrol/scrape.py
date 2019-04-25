from presscontrol.utils import tprint, mysql_engine, center_xcols
from presscontrol.db_utils import shuffle_table
from datetime import datetime, timedelta
import presscontrol.GetOldTweets as got
from presscontrol.config import config
import pandas as pd
import calendar
import pickle
import os
import re

def tweet2url(tweets=None, user='', days=0, months=0, years=0, 
              monthly=False, yearly=False, since='',until=''):
    '''Get URLs from a list of tweets (GetOldTweets class) or perform tweet scraping by specifying a user and dates.'''
    
    urls = []
    summary = Summary()

    if tweets == None:
        tweets = scrape_tweets(user=user, days=days, months=months, 
                               years=years, monthly=monthly,
                               yearly=yearly, since=since, until=until)
    for y in tweets.keys():
        for m in tweets[y].keys():
            urls_ = []
            for tweet in tweets[y][m]:
                urls_ += get_urls(tweet)
            urls_ = list(set(urls_))
            urls += urls_
            summary.add_entry(user=user, year=y, month=m, number=len(urls_))
                        
    return urls, summary


def get_urls(tweet):
    urls_tweet= tweet.urls.split(',')
    urls_text = re.findall(r'\b((?:https?://)?(?:(?:www\.)?(?:[\da-z\.-]+)\.(?:[a-z]{2,6})|(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)|(?:(?:[0-9a-fA-F]{1,4}:){7,7}[0-9a-fA-F]{1,4}|(?:[0-9a-fA-F]{1,4}:){1,7}:|(?:[0-9a-fA-F]{1,4}:){1,6}:[0-9a-fA-F]{1,4}|(?:[0-9a-fA-F]{1,4}:){1,5}(?::[0-9a-fA-F]{1,4}){1,2}|(?:[0-9a-fA-F]{1,4}:){1,4}(?::[0-9a-fA-F]{1,4}){1,3}|(?:[0-9a-fA-F]{1,4}:){1,3}(?::[0-9a-fA-F]{1,4}){1,4}|(?:[0-9a-fA-F]{1,4}:){1,2}(?::[0-9a-fA-F]{1,4}){1,5}|[0-9a-fA-F]{1,4}:(?:(?::[0-9a-fA-F]{1,4}){1,6})|:(?:(?::[0-9a-fA-F]{1,4}){1,7}|:)|fe80:(?::[0-9a-fA-F]{0,4}){0,4}%[0-9a-zA-Z]{1,}|::(?:ffff(?::0{1,4}){0,1}:){0,1}(?:(?:25[0-5]|(?:2[0-4]|1{0,1}[0-9]){0,1}[0-9])\.){3,3}(?:25[0-5]|(?:2[0-4]|1{0,1}[0-9]){0,1}[0-9])|(?:[0-9a-fA-F]{1,4}:){1,4}:(?:(?:25[0-5]|(?:2[0-4]|1{0,1}[0-9]){0,1}[0-9])\.){3,3}(?:25[0-5]|(?:2[0-4]|1{0,1}[0-9]){0,1}[0-9])))(?::[0-9]{1,4}|[1-5][0-9]{4}|6[0-4][0-9]{3}|65[0-4][0-9]{2}|655[0-2][0-9]|6553[0-5])?(?:/[\w\.-]*)*/?)\b', tweet.text)
    urls = urls_tweet+urls_text
    urls = ['http://'+url.replace('https://', '').replace('http://', '') for url in urls]
    urls = list(set(urls))
    urls = [u for u in urls if 
            u!='' and 
            'twitter.com' not in u and 
            'pic.twitter.com' not in u and 
            u!='http://']

    return urls


def scrape_tweets(user, days=0, months=0, years=0, monthly=False, 
                  yearly=False, since='', until=''):
    
    path = f"{os.environ['HOME']}/presscontrol/twitter_tempfiles"
    if not os.path.exists(path):
        os.makedirs(path)

    tweets = {}
    counter = 0
    
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
        
        
        if since_.year not in tweets.keys():
            tweets[since_.year] = {}

        until_ = next_date(since_) if next_date(since_) < until else until
        
        if since_.day == until_.day:
            pr = f'{calendar.month_name[since_.month]} {since_.year}'
        else:
            pr = f'{dt2str(since_)} to {dt2str(until_)}'
            
        filename = f"{path}/{user} {pr}.pkl"
        tprint(f'[·] Getting tweets from {pr}')
        
        if os.path.exists(filename):
            tprint('[+] Found in twitter tempfiles')
            tweets_ = pd.read_pickle(filename)
            
        else:
            try:
                tweetCriteria = got.manager.TweetCriteria().setUsername(user).setSince(dt2str(since_)).setUntil(dt2str(until_))
                tweets_ = got.manager.TweetManager.getTweets(tweetCriteria)
                
                if len(tweets_) > 0:
                    with open(f"{path}/{user} {pr}.pkl", 'wb') as f:
                        pickle.dump(tweets_, f)

            except Exception as exc:
                tweets_ = []
                print('\nError\n', exc)

        tprint(f'[+] Done ({len(tweets_)} tweets).')
        counter += len(tweets_)
        tweets[since_.year][since_.month] = tweets_
        since_ = until_
        
    print()
    tprint(f'[+] DONE ({counter} tweets)')
        
    return tweets


def links2db(urls, engine=None, con=None, display='', save='', update='', file_name=None, shuffle=False):

    df = pd.DataFrame(urls, columns=['original_link'])
    
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

    if display is True:
        print()
        big_str = '\n'.join(urls[:1000])
        more = f"echo '{big_str}' | more -10"
        os.system(more)

    if save is True:
        save_csv(df, file_name=file_name)
        
    if update is True:
        update_db(df, engine=engine, con=con, shuffle=shuffle)

        
def save_csv(df, file_name=None):
    print()
    if file_name is None:
        file_name = input('File name (implicit .csv): ~/presscontrol/')
    df.to_csv(f"{os.environ['HOME']}/presscontrol/{file_name}.csv", index=False, encoding='utf-8-sig', header=False)
    print(f'Articles Saved to ~/presscontrol/{file_name}.csv')
    print()

    
def update_db(df, backup=None, queue=None, engine=None, con=None, shuffle=False):
    if backup is None: backup = config['TABLES']['BACKUP']
    if queue is None: queue = config['TABLES']['QUEUE']
    if engine is None and con is None: engine = mysql_engine()
    if con is None: con = engine.connect()

    print()
    df.to_sql('erase', con=con, index=False, if_exists='append', chunksize=50000)

    engine.execute(f'insert ignore into {backup} (original_link) select original_link from erase')
    engine.execute(f'insert ignore into {queue} (original_link) select original_link from erase')

    engine.execute('drop table if exists erase')
    
    tprint('Successfully added urls to database.')
    
    shuffle_table(engine=engine, shuffle=shuffle)

    
class Summary:
    def __init__(self):
        self.sum = {}
        self.df = None
        
    def add_entry(self, user=None, year=None, month=None, number=None):
        if user is None:
            user = '?'
        if user not in self.sum.keys():
            self.sum[user] = {}
        if year not in self.sum[user].keys():
            self.sum[user][year] = {}
        
        if type(month) == int:
            month = calendar.month_abbr[month]
            
        self.sum[user][year][month] = number
        
    def add_dict(self, user, d):
        self.sum[user] = d[list(d.keys())[0]]
        
    def to_df(self):
        raw_list = []
        for user in self.sum.keys():
            for year in self.sum[user].keys():
                for month in self.sum[user][year].keys():
                    raw_list += [[user,year,month,self.sum[user][year][month]]]
        self.df = pd.DataFrame(raw_list, columns=['user', 'year', 'month', 'n'])
        
    def print(self):
        for user in self.sum.keys():
            print()
            print(f'  {user}  '.center(60, '='))
            print()
            for year in self.sum[user].keys():
                print(year,':')
                print()
                l = []
                for month in self.sum[user][year].keys():
                    l.append(month+': '+str(self.sum[user][year][month])+' links')
                center_xcols(l, width=60, ncols=3)
                
    def dump_csv(self):
        filename = f"{os.environ['HOME']}/presscontrol/twitter_scrape_1.csv"
        i = 1
        while os.path.exists(filename):
            filename = '_'.join(filename.split('_')[:-1])+f'_{i}.csv'
            i += 1
            
        if self.df is None:
            self.to_df()
        self.df.to_csv(filename, index=False, encoding='utf-8-sig')
        print(f'Summary saved to {filename}')


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
