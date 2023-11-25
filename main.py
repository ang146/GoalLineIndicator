from win11toast import notify
import schedule, time
from fetcher import Fetcher
from config import CHECKINTERVAL

def CreateNotify():
    results = fetcher.FindMatch()
    for result in results:
        notify(result[0], result[1], on_click=result[2])
    
if __name__ == "__main__":
    fetcher = Fetcher(True)
    CreateNotify()
    schedule.every(30).seconds.do(CreateNotify)
    #schedule.every(CHECKINTERVAL).minutes.do(CreateNotify)
    
    while True:
        schedule.run_pending()
        time.sleep(1)
        