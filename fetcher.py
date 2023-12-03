from bs4 import BeautifulSoup, Tag
from config import *
from typing import List
from recorder import Recorder
from crawler import Crawler, SiteApi
import utils
import queue, threading

FULLTIME_GOALLINE="hil"
HALFTIME_GOALLINE="fhl"

class Match:
    home_name = None
    away_name = None
    time_text = None
    time_int = None
    id = None
    is_started = None
    is_goaled = None
    is_first_half = None
    is_live_match = None
        
    def __init__(self, data :Tag):
        center_text = data.select_one(".text-center")
        self.home_name = utils.trim_string(data.find("div", class_=lambda c: "text-right" in c).text)
        self.away_name = utils.trim_string(data.find("div", class_=lambda c: "text-left" in c).text)
        self.id = data.find("a", href=True)['href'][7:-8]
        if "未開賽" in center_text.text:
            self.is_started = False
            return
        self.is_started = True
        
        self.is_live_match = data.find('span', class_=lambda c: "badge-danger" in c) != None
        self.is_goaled = center_text.find("div", class_="lead").text != "0-0"
        current_time = center_text.select_one(".text-danger").text
        if not current_time.startswith("半場"):
            self.time_text = current_time[3:]
            if len(current_time) > 6:
                if (current_time[3:].startswith("4") or current_time[3].startswith("5")):
                    self.time_int = 45
                else:
                    self.time_int = 90
            else:
                self.time_int = int(current_time[3:-1])
        else:
            self.time_text = "半場"
            self.time_int = 46
        self.is_first_half = self.time_int <= 45
        
    def __str__(self):
        max_length = 32
        home = utils.half_to_full_width(self.home_name) + " " * (max_length-len(self.home_name)*2)
        away = utils.half_to_full_width(self.away_name) + " " * (max_length-len(self.away_name)*2)
        return f'[{self.id}]{home} 對 {away} '

class Fetcher:
    half_time_fetch_cache = None
    full_time_fetch_cache = None
    recorder = None
    use_db = False
    fetch_counter = None
    keep_notifying = None
    
    def __init__(self, keep_notify :bool, connection_string :str = ""):
        if connection_string:
            self.recorder = Recorder(connection_string)
            self.use_db = True
        self.fetch_counter = 1
        self.half_time_fetch_cache = []
        self.full_time_fetch_cache = []
        self.keep_notifying = keep_notify
        
    def FillMatchResults(self) -> None:
        if not self.use_db:
            return
        
        ids = self.recorder.GetUnfinishedMatchIds()
        for id in ids:
            scores = Crawler.GetMatchResults(id)
            if len(scores) == 0:
                continue
            prematch_odds = Crawler.GetPreMatchOdds(id, True)
            prematch_goal_line = list(prematch_odds)[0]
            prematch_high_odd = prematch_odds[prematch_goal_line][0]
            prematch_odd_flow = prematch_odds[prematch_goal_line][1]
            self.recorder.WriteResult(id, 
                                      prematch_high_odd, 
                                      prematch_odd_flow,
                                      scores['ht'] != 0,
                                      True)
            
            prematch_odds = Crawler.GetPreMatchOdds(id, False)
            prematch_goal_line = list(prematch_odds)[0]
            prematch_high_odd = prematch_odds[prematch_goal_line][0]
            prematch_odd_flow = prematch_odds[prematch_goal_line][1]
            self.recorder.WriteResult(id, 
                                      prematch_high_odd, 
                                      prematch_odd_flow,
                                      scores['ft'] != 0,
                                      False)

    def FindMatch(self) -> List[List[str]]:
        print(f"進行第{self.fetch_counter}次fetching")
        result = Crawler.GetWebsiteData(SiteApi.G10OAL.value, SiteApi.G10OAL_Live_Mathces.value).text
        soup = BeautifulSoup(result, "html.parser")
        matches = soup.find_all(class_="card-body")
        toReturn = []
        
        print(f'搵到一共{len(matches)}場賽事')
        
        q = queue.Queue()
        for match in matches:
            q.put((match,))
            
        for i in range(16):
            thread = threading.Thread(target=self.__findmatch, args=(q, toReturn) )
            thread.daemon = True
            thread.start()
        
        q.join()
            
        self.fetch_counter += 1
        return toReturn
    
    def __findmatch(self, queue: queue.Queue, toReturn:list):
        try:
            while not queue.empty():
                match = queue.get()[0]
                
                m = Match(match)
                if not m.is_started:
                    print(f'{str(m)}未開場')
                    queue.task_done()
                    continue
                
                if m.is_goaled:
                    print(f'{str(m)}已入波')
                    queue.task_done()
                    continue
                    
                if not m.is_live_match:
                    print(f'{str(m)}無即場')
                    queue.task_done()
                    continue
                
                if m.is_first_half and m.id in self.half_time_fetch_cache:
                    print(f'{str(m)}上半場已通知')
                    queue.task_done()
                    continue
                if not m.is_first_half and m.id in self.full_time_fetch_cache:
                    print(f'{str(m)}下半場已通知')
                    queue.task_done()
                    continue
                
                odd = Crawler.GetLiveTimeOdds(m.id)
                if odd == -1:
                    print(f'{str(m)}搵唔到賠率')
                    queue.task_done()
                    continue
                
                if odd < 2:
                    print(f'{str(m)}目前賠率{odd}, 未達2水')
                    queue.task_done()
                    continue
                
                if odd > 2.15:
                    print(f'{str(m)}目前賠率超過2.15, 不會紀錄及通知')
                    queue.task_done()
                    continue
                
                prematch_odds = Crawler.GetPreMatchOdds(m.id, m.is_first_half)
                prematch_goal_line = list(prematch_odds)[0]
                prematch_high_odd = prematch_odds[prematch_goal_line][0]
                prematch_odd_flow = prematch_odds[prematch_goal_line][1]
                if prematch_odd_flow is None:
                    flow = '無升跌'
                elif prematch_odd_flow:
                    flow = '回飛'
                else:
                    flow = '落飛'
                win_rate = ""
                if self.use_db:
                    self.recorder.WriteMatch(m.id, m.time_int, odd, m.is_first_half)
                    exact_win_rate = self.recorder.GetSuccessRateByTime(m.time_int, m.time_int, m.is_first_half, prematch_odd_flow)
                    group_min_mins = int((m.time_int - 1)/5) * 5 + 1
                    group_win_rate = self.recorder.GetSuccessRateByTime(group_min_mins, group_min_mins + 4, m.is_first_half, prematch_odd_flow)
                    range_win_rate = self.recorder.GetSuccessRateByTime(m.time_int - 2, m.time_int + 2, m.is_first_half, prematch_odd_flow)
                    if exact_win_rate != -1:
                        win_rate += "\n於{time}分且{f}的成功率為: {rate:.2f}%".format(time= m.time_int,rate=exact_win_rate, f=flow)
                    if group_win_rate != -1:
                        win_rate += "\n於{time_min}分至{time_max}分且{f}的成功率為: {rate:.2f}%".format(time_min= group_min_mins, time_max= group_min_mins + 4, rate=group_win_rate, f =flow)
                    if range_win_rate != -1:
                        win_rate += "\n於{time}前後2分鐘且{f}的成功率為: {rate:.2f}%".format(time= m.time_int, rate=range_win_rate, f=flow)
                        
                header = f'{m.home_name} 對 {m.away_name} 即場0.75大有水'
                body = f'目前球賽時間 {m.time_text}\n'
                body += f'目前賠率: 0.5/1.0大 - {odd}\n'
                body += f'賽前賠率: {prematch_goal_line}大 - {prematch_high_odd}, {flow}\n'
                body += f'{win_rate}'
                print(f'{str(m)}將發出通知')
                toReturn.append([header, body])
                
                if m.is_first_half:
                    if len(self.half_time_fetch_cache) > 50:
                        _ = self.half_time_fetch_cache.pop(0)
                        
                    self.half_time_fetch_cache.append(m.id)
                else:
                    if len(self.full_time_fetch_cache) > 50:
                        _ = self.full_time_fetch_cache.pop(0)
                        
                    self.full_time_fetch_cache.append(m.id)
                    
                queue.task_done()
        except Exception:
            print("Unknow exception occurred. Gracefully abort process")
            queue.task_done()
    
if __name__ == "__main__":
    from crawler import Crawler
    result = Crawler.GetWebsiteData("http://g10oal.com", "/live").text
    soup = BeautifulSoup(result, "html.parser")
    matches = soup.find_all(class_="card-body")
    m = Match(matches[7])
    print('主隊 ' + m.home_name)
    print('客隊 '+ m.away_name)
    print('Id ' + m.id)
    print('時間(int) ' + str(m.time_int))
    print('時間 ' + m.time_text)
    print('開咗場 ' + str(m.is_started))
    print('入咗波 ' + str(m.is_goaled))
    print('有即場 ' + str(m.is_live_match))
    print('上半場 ' + str(m.is_first_half))