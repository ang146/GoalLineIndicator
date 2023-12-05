from bs4 import BeautifulSoup, Tag
from config import *
from typing import List
from DataAccess.ResultRepository import ResultRepository
from DataAccess.ResultDto import ResultDto
from crawler import Crawler, SiteApi
import utils
import queue, threading

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
    repository = None
    fetch_counter = None
    
    def __init__(self, connection_string :str):
        self.repository = ResultRepository(connection_string)
        self.fetch_counter = 1
        self.half_time_fetch_cache = []
        self.full_time_fetch_cache = []
        
    def FillMatchResults(self) -> None:        
        dtos = self.repository.GetResults(False)
        for dto in dtos:
            if dto.ft_success is None or dto.ht_success is None or dto.ft_prematch_goalline is None or dto.ht_prematch_goalline is None:
                scores = Crawler.GetMatchResults(dto.id)
                if len(scores) == 0:
                    continue
                prematch_odds = Crawler.GetPreMatchOdds(dto.id, True)
                ht_goalline = list(prematch_odds)[0]
                dto.ht_prematch_goalline = ht_goalline
                dto.ht_prematch_odd = prematch_odds[ht_goalline][0]
                dto.ht_rise = prematch_odds[ht_goalline][1]
                dto.ht_success = scores['ht'] != 0
                    
                prematch_odds = Crawler.GetPreMatchOdds(dto.id, False)
                ft_goalline = list(prematch_odds)[0]
                dto.ft_prematch_goalline = ft_goalline
                dto.ft_prematch_odd = prematch_odds[ft_goalline][0]
                dto.ft_rise = prematch_odds[ft_goalline][1]
                dto.ft_success = scores['ft'] != 0
                    
                self.repository.Upsert(dto)

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
                new_dto = ResultDto(m.id, m.time_int, odd)
                self.repository.Upsert(new_dto)
                
                def GetSuccessRate(time_min :int, time_max :int, check_goalline :bool, is_first_half:bool) -> float:
                    previous_records = self.repository.GetResults(True)
                    if is_first_half:
                        if check_goalline:
                            target_results = [x for x in previous_records if (x.ht_time <= time_max and x.ht_time >= time_min and x.ht_prematch_goalline == prematch_goal_line)]
                        else:
                            target_results = [x for x in previous_records if (x.ht_time <= time_max and x.ht_time >= time_min)]
                        success = len([x for x in target_results if x.ht_success])
                    else:
                        if check_goalline:
                            target_results = [x for x in previous_records if (x.ft_time <= time_max and x.ft_time >= time_min and x.ft_prematch_goalline == prematch_goal_line)]
                        else:
                            target_results = [x for x in previous_records if (x.ft_time <= time_max and x.ft_time >= time_min)]
                        success = len([x for x in target_results if x.ft_success])
                    
                    total = len(target_results)
                    
                    if total < 10:
                        return -1
                    
                    return success/total * 100
                
                exact_win_rate = GetSuccessRate(m.time_int, m.time_int, False, m.is_first_half)
                range_win_rate = GetSuccessRate(m.time_int - 2, m.time_int + 2, False, m.is_first_half)
                goalline_before_win_rate = GetSuccessRate(0 if m.is_first_half else 46, m.time_int, True, m.is_first_half)
                goalline_after_win_rate = GetSuccessRate(m.time_int, 45 if m.is_first_half else 90, True, m.is_first_half)
                goalline_between_win_rate = GetSuccessRate(m.time_int - 2, m.time_int + 2, True, m.is_first_half)
                if exact_win_rate != -1:
                    win_rate += "\n於{time}的成功率為: {rate:.2f}%".format(time= m.time_text,rate=exact_win_rate, f=flow)
                if range_win_rate != -1:
                    win_rate += "\n於{time}前後2分鐘的成功率為: {rate:.2f}%".format(time= m.time_text, rate=range_win_rate, f=flow)
                if goalline_before_win_rate != -1:
                    win_rate += "\n中位數{goalline}球於{time}前發出的成功率: {rate:.2f}%".format(goalline=prematch_goal_line, time=m.time_text, rate=goalline_before_win_rate)
                if goalline_after_win_rate != -1:
                    win_rate += "\n中位數{goalline}球於{time}後發出的成功率: {rate:.2f}%".format(goalline=prematch_goal_line, time=m.time_text, rate=goalline_after_win_rate)
                if goalline_between_win_rate != -1:
                    win_rate += "\n中位數{goalline}球於{time}前後2分鐘發出的成功率: {rate:.2f}%".format(goalline=prematch_goal_line, time=m.time_text, rate=goalline_between_win_rate)
                
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