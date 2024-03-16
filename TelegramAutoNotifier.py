import telegram
import schedule, time
from Fetcher import Fetcher
from Config import *
import asyncio, threading
from LoggerFactory import LoggerFactory

def GetCurrentTime() -> str:
    return time.strftime('%Y-%m-%dT%H:%M:%SZ', time.localtime())
    
async def SendNotificationToTelegramAsync():
    global fetching
    fetching = True
    logger.debug("開始取得即場賽事資料, 檢查各場次")
    print(f"[{GetCurrentTime()}]檢查各場次")
    results = fetcher.FindMatch()
    logger.debug(f"需發通知場次數 {len(results)}")
    print(f"[{GetCurrentTime()}]需發通知場次數 {len(results)}")
    fetching = False
    for result in results:
        msg = result[0] + '\n' + result[1]
        bot = telegram.Bot(token=TOKEN)
        await bot.send_message(GROUP_ID, text=msg)
        
def ResultsFetchNewThread():
    logger.debug("正在取得完場賽事資料")
    t = threading.Thread(target=fetcher.FillMatchResults)
    t.start()
    
def OddsFetchAsync():
    asyncio.run(SendNotificationToTelegramAsync()) 
    
def OddsFetchNewThread():
    logger.debug("準備開始取得即場賽事資料")
    if not fetching:
        t = threading.Thread(target=OddsFetchAsync)
        t.start()
    else:
        logger.debug("上一個Thread仍然進行中, 取消取得程序")
        print("上一個Thread仲fetch緊")
    
if __name__ == "__main__":
    global fetcher
    global logger
    loggingFactory = LoggerFactory("AutoNotifier_Logs")
    logger = loggingFactory.getLogger("Main")
    fetcher = Fetcher(CONNECTION_STRING, loggingFactory)
    ResultsFetchNewThread()
    OddsFetchAsync()
    schedule.every(CHECKINTERVAL_SECOND).seconds.do(OddsFetchNewThread)
    schedule.every(CHECKINTERVAL_MINUTES).minutes.do(ResultsFetchNewThread)
    
    while True:
        schedule.run_pending()
        time.sleep(1)