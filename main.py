import telegram
import schedule, time
from fetcher import Fetcher
from config import *
import asyncio, threading

DRIVER_NAME = 'SQL SERVER'
CONNECTION_STRING = f"""
    DRIVER={{{DRIVER_NAME}}};
    SERVER={SERVER_NAME};
    DATABASE={DATABASE_NAME};
    PORT=49172;
    Trust_Connection=yes;
    uid={USER_NAME};
    pwd={PASSWORD};
"""

def GetCurrentTime() -> str:
    return time.strftime('%Y-%m-%dT%H:%M:%SZ', time.localtime())
    
async def SendNotificationToTelegramAsync():
    global fetching
    fetching = True
    print(f"[{GetCurrentTime()}]檢查各場次")
    results = fetcher.FindMatch()
    print(f"[{GetCurrentTime()}]需發通知場次數 {len(results)}")
    for result in results:
        msg = result[0] + '\n' + result[1]
        bot = telegram.Bot(token='6053673668:AAGqaWrfrALlOF6XT7g2aNfONW7jjyXFRZk')
        await bot.send_message(-1002143736198, text=msg)
    fetching = False
        
def threading_results_fetch():
    t = threading.Thread(target=fetcher.FillMatchResults)
    t.start()
    
def async_odd_fetch():
    asyncio.run(SendNotificationToTelegramAsync()) 
    
def threading_odds_fetch():
    if not fetching:
        t = threading.Thread(target=async_odd_fetch)
        t.start()
    else:
        print("上一個Thread仲fetch緊")
    
if __name__ == "__main__":
    global fetcher
    fetcher = Fetcher(CONNECTION_STRING)
    threading_results_fetch()
    async_odd_fetch()
    schedule.every(CHECKINTERVAL_SECOND).seconds.do(threading_odds_fetch)
    schedule.every(CHECKINTERVAL_MINUTES).minutes.do(threading_results_fetch)
    
    while True:
        schedule.run_pending()
        time.sleep(1)