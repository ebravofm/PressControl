import PressControl.got3 as got3
from datetime import datetime, timedelta
from PressControl.utils import tprint, mysql_engine, read_config
import pandas as pd
import os

def tweet2url(tweet_list=None, 
              username=None, 
              days=0, 
              months=0, 
              years=0, 
              monthly=False, 
              yearly=False, 
              since='',
              until=''):
    
    if tweet_list == None:
        tweet_list = scrape_tweets(username=username, days=days, 
                                   months=months, years=years, monthly=monthly, 
                                   yearly=yearly, since=since, until=until)
    urls = []
    for tweet in tweet_list:
        urls += tweet.urls.split(',')
        
    urls = [u for u in urls if u!='']
    urls = list(set(urls))
            
    return urls


def scrape_tweets(username, days=0, months=0, years=0, monthly=False, 
                  yearly=False, since='', until=''):

    tweets = []
    
    if until == '':
        until_dt = datetime.today()+timedelta(1)
        until = until_dt.strftime('%Y-%m-%d')
    else:
        until_dt = datetime.strptime(until, '%Y-%m-%d')
        
    if since == '':
        delta = int(days + months*31 + years*365)
        since_dt = until_dt - timedelta(delta)
        since = since_dt.strftime('%Y-%m-%d')
    else:
        since_dt = datetime.strptime(since, '%Y-%m-%d')


    since_dt_loop = since_dt
    while since_dt_loop < until_dt:
        
        since_loop = since_dt_loop.strftime('%Y-%m-%d')
        until_dt_loop = (since_dt_loop + timedelta(31)) if (since_dt_loop + timedelta(31)) < until_dt else until_dt
        until_loop = until_dt_loop.strftime('%Y-%m-%d')
        
        tprint('[·] Getting tweets from %s to %s' % (since_loop, until_loop))

        tweetCriteria = got3.manager.TweetCriteria()
        
        tweetCriteria.username = username
        tweetCriteria.since = since_loop
        tweetCriteria.until = until_loop

        tweets_loop = got3.manager.TweetManager.getTweets(tweetCriteria)
        tprint('[+] Done (%d tweets).' % len(tweets_loop))

        tweets += tweets_loop
        since_dt_loop += timedelta(31)
        
    tprint('[+] DONE (%d tweets)' % len(tweets))
        
    return tweets


def links2db(urls, con=None, config=None):
    if config == None:
        config = read_config()
    backup = config['TABLES']['BACKUP']
    queue = config['TABLES']['QUEUE']

    if con == None:
        engine = mysql_engine()
        con = engine.connect()

    engine.execute('drop table if exists erase')
        
    df = pd.DataFrame(urls, columns=['original_link'])
    
    display = input(f'Display {len(df)} links? (y/n): ')
    
    if display == 'y':
        print()
        big_str = '\n'.join(urls)
        os.system(f"echo '{big_str}' | more")
                    
    
    save = input(f'Save {len(df)} links to csv file? (y/n): ')
    if save == 'y':
        print()
        file_name = input('File name (implicit .csv): ~/PressControl/')
        df.to_csv(f"{os.environ['HOME']}/PressControl/{file_name}.csv", index=False, encoding='utf-8-sig', header=False)
        print(f'Articles Saved to ~/PressControl/{file_name}.csv')
        print()
        
    
    update = input(f'Add {len(df)} links to database (y/n): ')
    if update == 'y':
        print()
        df.to_sql('erase', con=con, index=False, if_exists='append')

        engine.execute(f'insert ignore into {backup} (original_link) select original_link from erase')
        engine.execute(f'insert ignore into {queue} (original_link) select original_link from erase')

        engine.execute('drop table if exists erase')
        
        print('Success.')
        
        shuffle_queue(engine=engine, config=config)

    
def shuffle_queue(engine=None, config=None):
    if config == None:
        config = read_config()
    queue = config['TABLES']['QUEUE']
        
    print()
    x = input('Shuffle queue? Recomended to prevent IP being banned (y/n): ')
    print()
    
    if x == 'y':

        if engine == None:
            engine = mysql_engine()

        temp = f'{queue}_backup_'+datetime.now().strftime('%d_%m')

        tprint('[·] Shuffling queue table (can take up to 5 mins).')

        engine.execute(f'create table {temp} like {queue}')
        engine.execute(f'insert into {temp} (original_link) select original_link from queue order by rand()')
                
        engine.execute(f'drop table {queue}')
        engine.execute(f'rename table {temp} to {queue}')
        tprint('[+] Done shuffling.')