from bs4 import BeautifulSoup, Tag
from config import *
from typing import List
from DataAccess.ResultRepository import ResultRepository
from DataAccess.ResultDto import ResultDto
from crawler import Crawler, SiteApi
import utils
import queue, threading
import time
from datetime import datetime, timedelta

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
    date = None
        
    def __init__(self, data :Tag):
        center_text = data.select_one(".text-center")
        self.home_name = utils.trim_string(data.find("div", class_=lambda c: "text-right" in c).text)
        self.away_name = utils.trim_string(data.find("div", class_=lambda c: "text-left" in c).text)
        self.id = data.find("a", href=True)['href'][7:-8]
        self.date = self._convert_string_to_datetime(data.find("h6", class_=lambda c: "text-muted" in c).find('small').text[:-6])
        if "未開賽" in center_text.text:
            self.is_started = False
            return
        self.is_started = True
        
        live_badge = data.find('span', class_=lambda c: "badge-danger" in c)
        self.is_live_match = live_badge != None and "即場" in live_badge.text
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
        
    def _convert_string_to_datetime(self, month_day_string):
        # Get today's date
        today = datetime.now()

        # Parse the provided month and day string (assuming it's in MM-DD format)
        month_day = datetime.strptime(month_day_string, "%m-%d")

        # Check if the parsed month and day are after today's date
        # If so, assume it's from the previous year
        if datetime(today.year, month_day.month, month_day.day) > today:
            year = today.year - 1
        else:
            year = today.year

        # Combine the parsed month and day with the determined year
        converted_date = datetime(year, month_day.month, month_day.day)
        return converted_date
        
    def __str__(self):
        max_length = 32
        home = utils.half_to_full_width(self.home_name) + " " * (max_length-len(self.home_name)*2)
        away = utils.half_to_full_width(self.away_name) + " " * (max_length-len(self.away_name)*2)
        return f'[{self.id}]{home} 對 {away} '

class Fetcher:    
    def __init__(self, connection_string :str, loggerFactory):
        self.repository = ResultRepository(connection_string, loggerFactory)
        self.fetch_counter = 1
        self.half_time_fetch_cache = []
        self.full_time_fetch_cache = []
        self.ht_last_min = []
        self.ft_last_min = []
        self.loggerFactory = loggerFactory
        self.logger = loggerFactory.getLogger("Fetcher")
        self.crawler = Crawler(loggerFactory)
        
    def FillMatchResults(self) -> None:
        dtos = self.repository.GetResults(False)
        self.logger.debug(f"已取得{len(dtos)}項已保存賽事")
        for dto in dtos:
            if dto.ft_success is None or dto.ht_success is None:
                self.logger.debug(f"{dto.id}紀錄未齊全, 將補完賽事")
                scores = self.crawler.GetMatchResults(dto.id)
                if len(scores) == 0:
                    self.logger.debug(f"無法取得{dto.id}賽事賽果, 將跳過")
                    continue
                prematch_odds = self.crawler.GetPreMatchOdds(dto.id, True)
                ht_goalline = list(prematch_odds)[0]
                dto.ht_prematch_goalline = ht_goalline
                dto.ht_prematch_odd = prematch_odds[ht_goalline][0]
                dto.ht_rise = prematch_odds[ht_goalline][1]
                dto.ht_success = scores['ht'] != 0
                self.logger.debug(f"已取得{dto.id}賽事半場賽果: 半場入球{scores['ht']}, 中位數入球{ht_goalline}, 入球大賠率{dto.ht_prematch_odd}, 賠率流向為上升{dto.ht_rise}")
                    
                prematch_odds = self.crawler.GetPreMatchOdds(dto.id, False)
                ft_goalline = list(prematch_odds)[0]
                dto.ft_prematch_goalline = ft_goalline
                dto.ft_prematch_odd = prematch_odds[ft_goalline][0]
                dto.ft_rise = prematch_odds[ft_goalline][1]
                dto.ft_success = scores['ft'] != 0
                self.logger.debug(f"已取得{dto.id}賽事全場賽果: 全半場入球{scores['ht']}, 中位數入球{ft_goalline}, 入球大賠率{dto.ft_prematch_odd}, 賠率流向為上升{dto.ft_rise}")
                    
                self.repository.Upsert(dto)

    def FindMatch(self) -> List[List[str]]:
        self.logger.debug(f"進行第{self.fetch_counter}次fetching")
        print(f"進行第{self.fetch_counter}次fetching")
        result = self.crawler.GetWebsiteData(SiteApi.G10OAL.value, SiteApi.G10OAL_Live_Mathces.value).text
        soup = BeautifulSoup(result, "html.parser")
        matches_text = soup.find_all(class_="card-body")
        matches :List[Match] = []
        today_date = datetime.now().date()
        self.logger.debug(f'搵到一共{len(matches_text)}場賽事')
        print(f'搵到一共{len(matches_text)}場賽事')
        for match_text in matches_text:
            m = Match(match_text)
            self.logger.debug(f"{m.id}賽事日期為{str(m.date.date())}")
            if (today_date - m.date.date() ).days <= 1:
                matches.append(m)
        toReturn = []
        
        if len(matches) == 0:
            self.logger.debug("沒有任何賽事數據, 將閒置Thread 20分鐘")
            time.sleep(1200)
        else:            
            if all(not x.is_started for x in matches) or all(not x.is_live_match or x.is_goaled or not x.is_started for x in matches):
                self.logger.debug("所有賽事均未開賽, 或已開賽但沒有即場或已入球, 將閒置Thread 1分鐘")
                time.sleep(60)
            elif all(not x.is_live_match for x in matches) or all(x.is_goaled for x in matches) or all(not x.is_live_match or x.is_goaled for x in matches):
                self.logger.debug("所有賽事均無即場或已入球, 將閒置Thread 15分鐘")
                time.sleep(900)
        
        self.logger.debug(f'將檢查共{len(matches)}場賽事')
        print(f'將檢查共{len(matches)}場賽事')
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
                m :Match = queue.get()[0]
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
                
                if m.time_int >= 41 and m.is_first_half and m.id not in self.ht_last_min:
                    header = f'{m.home_name} 對 {m.away_name} 半場最後4分鐘'
                    body = f'目前球賽時間 {m.time_text}'
                    toReturn.append((header, body))
                    self.ht_last_min.append(m.id)
                    if len(self.ht_last_min) > 5:
                        _ = self.ht_last_min.pop(0)
                        
                if m.time_int >= 86 and not m.is_first_half and m.id not in self.ft_last_min:
                    header = f'{m.home_name} 對 {m.away_name} 全場最後4分鐘'
                    body = f'目前球賽時間 {m.time_text}'
                    toReturn.append((header, body))
                    self.ft_last_min.append(m.id)
                    if len(self.ft_last_min) > 5:
                        _ = self.ft_last_min.pop(0)
                
                if m.is_first_half and m.id in self.half_time_fetch_cache:
                    print(f'{str(m)}上半場已通知')
                    queue.task_done()
                    continue
                if not m.is_first_half and m.id in self.full_time_fetch_cache:
                    print(f'{str(m)}下半場已通知')
                    queue.task_done()
                    continue
                
                self.logger.debug(f"{m.id}賽事有即場, 目前比分為0-0, 且未進行通知. 將取得即場賠率")
                odd = self.crawler.GetLiveTimeOdds(m.id)
                if odd == -1:
                    self.logger.debug(f"{m.id}賽事沒有即場0.75大球賠率")
                    print(f'{str(m)}搵唔到賠率')
                    queue.task_done()
                    continue
                
                if odd < 2:
                    self.logger.debug(f"{m.id}賽事0.75大賠率未達2倍")
                    print(f'{str(m)}目前賠率{odd}, 未達2水')
                    queue.task_done()
                    continue
                
                if odd > 2.15:
                    self.logger.debug(f"{m.id}賽事0.75大賠率超過2.15, 不宜紀錄")
                    print(f'{str(m)}目前賠率超過2.15, 不會紀錄及通知')
                    queue.task_done()
                    continue
                
                self.logger.debug(f"{m.id}賽事附合要求, 將取得賽前賠率並計算成功率")
                prematch_odds = self.crawler.GetPreMatchOdds(m.id, m.is_first_half)
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
                
                dto = self.repository.GetResultById(m.id)
                if dto is None:
                    self.logger.debug(f"{m.id}賽事為新增項目, 將新增至資料庫")
                    new_dto = ResultDto(m.id, m.time_int, odd)
                    self.repository.Upsert(new_dto)
                elif not m.is_first_half:
                    self.logger.debug(f"{m.id}為下半場賽事, 將更新資料庫")
                    dto.ft_time = m.time_int
                    dto.ft_odd = odd
                    self.repository.Upsert(dto)
                
                def GetSuccessRate(time_min :int, time_max :int, check_goalline :bool, is_first_half:bool, prematch_odd :float = None) -> float:
                    previous_records = self.repository.GetResults(True)
                    if is_first_half:
                        if check_goalline:
                            if prematch_odd is not None:
                                target_results = [x for x in previous_records if (x.ht_time <= time_max and x.ht_time >= time_min and x.ht_prematch_goalline == prematch_goal_line and x.ht_prematch_odd >= prematch_odd - 0.09 and x.ht_prematch_odd <= prematch_odd + 0.09)]
                            else:
                                target_results = [x for x in previous_records if (x.ht_time <= time_max and x.ht_time >= time_min and x.ht_prematch_goalline == prematch_goal_line)]
                        else:
                            target_results = [x for x in previous_records if (x.ht_time <= time_max and x.ht_time >= time_min)]
                        success = len([x for x in target_results if x.ht_success])
                    else:
                        filtered_records = [x for x in previous_records if (x.ft_time is not None)]
                        if check_goalline:
                            if prematch_odd is not None:
                                target_results = [x for x in filtered_records if (x.ft_time <= time_max and x.ft_time >= time_min and x.ft_prematch_goalline == prematch_goal_line and x.ft_prematch_odd >= prematch_odd - 0.09 and x.ft_prematch_odd >= prematch_odd + 0.09)]
                            else:
                                target_results = [x for x in filtered_records if (x.ft_time <= time_max and x.ft_time >= time_min and x.ft_prematch_goalline == prematch_goal_line)]
                        else:
                            target_results = [x for x in filtered_records if (x.ft_time <= time_max and x.ft_time >= time_min)]
                        success = len([x for x in target_results if x.ft_success])
                    
                    total = len(target_results)
                    
                    if total < 10:
                        return -1
                    
                    return success/total * 100
                
                goalline_and_odd_before_win_rate = GetSuccessRate(0 if m.is_first_half else 46, m.time_int + 2, True, m.is_first_half, prematch_high_odd)
                goalline_and_odd_after_win_rate = GetSuccessRate(m.time_int - 2, 45 if m.is_first_half else 90, True, m.is_first_half, prematch_high_odd)
                goalline_and_odd_between_win_rate = GetSuccessRate(m.time_int - 2, m.time_int + 2, True, m.is_first_half, prematch_high_odd)
                if goalline_and_odd_before_win_rate != -1 and goalline_and_odd_after_win_rate != -1 and goalline_and_odd_between_win_rate != -1:
                    win_rate += "\n中位數{goalline}球 賽前賠率於{prematch_odd}+-0.09的成功率:".format(goalline=prematch_goal_line, prematch_odd = prematch_high_odd)
                    win_rate += "\n{time}'前 - {rate:.2f}%".format(time=m.time_int + 2, rate=goalline_and_odd_before_win_rate)
                    win_rate += "\n{time}'後 - {rate:.2f}%".format(time=m.time_int -2, rate=goalline_and_odd_after_win_rate)
                    win_rate += "\n於{time}前後2分鐘 - {rate:.2f}%".format(time=m.time_text, rate=goalline_and_odd_between_win_rate)
                else:
                    goalline_before_win_rate = GetSuccessRate(0 if m.is_first_half else 46, m.time_int + 2, True, m.is_first_half)
                    goalline_after_win_rate = GetSuccessRate(m.time_int - 2, 45 if m.is_first_half else 90, True, m.is_first_half)
                    goalline_between_win_rate = GetSuccessRate(m.time_int - 2, m.time_int + 2, True, m.is_first_half)
                    if goalline_before_win_rate != -1 and goalline_after_win_rate != -1 and goalline_between_win_rate != -1:
                        win_rate += "\n中位數{goalline}球的成功率:".format(goalline=prematch_goal_line, prematch_odd = prematch_high_odd)
                        win_rate += "\n{time}'前 - {rate:.2f}%".format(time=m.time_int + 2, rate=goalline_before_win_rate)
                        win_rate += "\n{time}'後 - {rate:.2f}%".format(time=m.time_int -2, rate=goalline_after_win_rate)
                        win_rate += "\n於{time}前後2分鐘 - {rate:.2f}%".format(time=m.time_text, rate=goalline_between_win_rate)
                    else:
                        exact_win_rate = GetSuccessRate(m.time_int, m.time_int, False, m.is_first_half)
                        range_win_rate = GetSuccessRate(m.time_int - 2, m.time_int + 2, False, m.is_first_half)
                        if exact_win_rate != -1:
                            win_rate += "\n於{time}的成功率為: {rate:.2f}%".format(time= m.time_text,rate=exact_win_rate, f=flow)
                        if range_win_rate != -1:
                            win_rate += "\n於{time}前後2分鐘的成功率為: {rate:.2f}%".format(time= m.time_text, rate=range_win_rate, f=flow)
                
                header = f'{m.home_name} 對 {m.away_name} 即場0.75大有水'
                body = f'目前球賽時間 {m.time_text}\n'
                body += f'目前賠率: 0.5/1.0大 - {odd}\n'
                body += f'賽前賠率: {prematch_goal_line}大 - {prematch_high_odd}, {flow}\n'
                body += f'{win_rate}'
                self.logger.debug(f"{m.id}賽事將發出通知")
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
        except Exception as ex:
            self.logger.error(f"取得{m.id}賽事賠率期間發生錯誤. 錯誤類型:{type(ex)}. 錯誤內容:{ex}, {ex.args}")
            print("Unknow exception occurred. Gracefully abort process")
            queue.task_done()
    
if __name__ == "__main__":
    from crawler import Crawler
    from LoggerFactory import LoggerFactory
    loggerFact = LoggerFactory()
    crawler = Crawler(loggerFact)
    
    result = crawler.GetWebsiteData("http://g10oal.com", "/live").text
    soup = BeautifulSoup(result, "html.parser")
    matches = soup.find_all(class_="card-body")
    m = Match(matches[0])
    print('主隊 ' + m.home_name)
    print('客隊 '+ m.away_name)
    print('Id ' + m.id)
    #print('時間(int) ' + str(m.time_int))
    #print('時間 ' + m.time_text)
    print('開咗場 ' + str(m.is_started))
    #print('入咗波 ' + str(m.is_goaled))
    #print('有即場 ' + str(m.is_live_match))
    #print('上半場 ' + str(m.is_first_half))
    print('日期' + str(m.date))