import telegram
import schedule, time
from fetcher import Fetcher
from config import *
import asyncio

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
    print(f"[{GetCurrentTime()}]檢查各場次")
    results = fetcher.FindMatch()
    print(f"[{GetCurrentTime()}]需發通知場次數 {len(results)}")
    for result in results:
        msg = result[0] + '\n' + result[1] + '\n狗會網頁版:\n' + result[2]
        bot = telegram.Bot(token='6053673668:AAGqaWrfrALlOF6XT7g2aNfONW7jjyXFRZk')
        await bot.send_message(-1002143736198, text=msg)
    
def async_main_start():
    asyncio.run(SendNotificationToTelegramAsync()) 
    
if __name__ == "__main__":
    global fetcher
    fetcher = Fetcher(False, CONNECTION_STRING)
    async_main_start()
    schedule.every(CHECKINTERVAL).minutes.do(async_main_start)
    
    while True:
        schedule.run_pending()
        time.sleep(1)