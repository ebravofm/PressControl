from crontab import CronTab
import os

def show_pc_tasks(show=False):
    cron  = CronTab(user=True)
    
    text = []
    for job in cron:
        if 'presscontrol' in str(job): 
            text += [str(job)]
            if show is True:
                print(job)
        
    return text


def daily_scrape_twitter(usernames):
    path = f"{os.environ['HOME']}/presscontrol/cron-tasks/"
    if not os.path.exists(path):
         os.makedirs(path)
    cron  = CronTab(user=True)
    
    for username in usernames:
        cmd = f"/usr/local/bin/presscontrol --scrape -username {username} -days 2 --update >> {path+username}.log 2>&1"
        job = cron.new(command = cmd)
        job.setall('0 0 * * *')

    cron.write()
