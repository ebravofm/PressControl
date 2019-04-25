from crontab import CronTab

cron  = CronTab(user=True)

for job in cron:
    print(job)
    
    
job = cron.new(command = "python3 -c 'print(\"hola\")' >> ~/caca.txt")

job.minute.every(1)

cron.write()
