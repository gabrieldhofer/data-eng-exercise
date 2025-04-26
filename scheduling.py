import time
import schedule
import datetime

def job():
    print("**job**   " + str(datetime.datetime.now()))

schedule.every(10).seconds.do(job)

while True: 
    schedule.run_pending()
    time.sleep(1)
