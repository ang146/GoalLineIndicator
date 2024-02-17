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
        
    def __init__(self, data :Tag, data_site:str):
        if data_site == SiteApi.HK33.name:
            pass
        else:
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
            score = center_text.find("div", class_="lead")
            if (score is not None and score.text != "0-0"):
                self.is_goaled = True
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
                #print(f"{dto.id}紀錄未齊全, 將補完賽事")
                scores = self.crawler.GetMatchResults(dto.id)
                if len(scores) == 0:
                    self.logger.debug(f"無法取得{dto.id}賽事賽果, 將跳過")
                    continue
                prematch_odds = self.crawler.GetPreMatchOdds(dto.id)
                ht_goalline = list(prematch_odds['ht'])[0]
                dto.ht_prematch_goalline = ht_goalline
                dto.ht_prematch_odd = prematch_odds['ht'][ht_goalline][0]
                dto.ht_rise = prematch_odds['ht'][ht_goalline][1]
                dto.ht_success = scores['ht']
                self.logger.debug(f"已取得{dto.id}賽事半場賽果: 半場入球{scores['ht']}, 中位數入球{ht_goalline}, 入球大賠率{dto.ht_prematch_odd}, 賠率流向為上升{dto.ht_rise}")
                #print(f"已取得{dto.id}賽事半場賽果: 半場入球{scores['ht']}, 中位數入球{ht_goalline}, 入球大賠率{dto.ht_prematch_odd}, 賠率流向為上升{dto.ht_rise}")
                
                ft_goalline = list(prematch_odds['ft'])[0]
                dto.ft_prematch_goalline = ft_goalline
                dto.ft_prematch_odd = prematch_odds['ft'][ft_goalline][0]
                dto.ft_rise = prematch_odds['ft'][ft_goalline][1]
                dto.ft_success = scores['ft']
                self.logger.debug(f"已取得{dto.id}賽事全場賽果: 全半場入球{scores['ft']}, 中位數入球{ft_goalline}, 入球大賠率{dto.ft_prematch_odd}, 賠率流向為上升{dto.ft_rise}")
                #print(f"已取得{dto.id}賽事全場賽果: 全半場入球{scores['ft']}, 中位數入球{ft_goalline}, 入球大賠率{dto.ft_prematch_odd}, 賠率流向為上升{dto.ft_rise}")
                
                self.repository.Upsert(dto)
                
    def _SleepThread(self, message:str, sleep_time:int):
        self.logger.info(f"{message}, 將閒置Thread {str(sleep_time)}分鐘")
        time.sleep(sleep_time*60)
        self.fetch_counter += 1

    def FindMatch(self) -> List[List[str]]:
        self.logger.debug(f"進行第{self.fetch_counter}次fetching")
        print(f"進行第{self.fetch_counter}次fetching")
        try:
            result = self.crawler.GetWebsiteData(SiteApi.G10OAL.value, SiteApi.G10OAL_Live_Mathces.value).text
        except Exception as ex:
            self.logger.debug(f"從網頁取得資料失敗, 類別: {type(ex)}, {ex}, {ex.args}")
            return []
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
            self._SleepThread("沒有任何賽事數據", 20)
            return toReturn
        else:
            if not any(not x.is_started for x in matches) and (all(not x.is_live_match for x in matches) or all(x.is_goaled for x in matches) or all(not x.is_live_match or x.is_goaled for x in matches)):
                self._SleepThread("所有賽事均無即場或已入球", 30)
                return toReturn
            elif all(not x.is_started for x in matches) or all(not x.is_live_match or x.is_goaled or not x.is_started for x in matches):
                self._SleepThread("所有賽事均未開賽, 或已開賽但沒有即場或已入球", 1)
                return toReturn
        
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
    
    def __getOddIncrement(self, odd:float):
        if odd < 1.5:
            return 0.03
        if odd >= 1.5 and odd < 1.65:
            return 0.05
        if odd >= 1.65 and odd < 1.8:
            return 0.07
        if odd >= 1.8 and odd < 2.05:
            return 0.09
        if odd >= 2.05 and odd < 2.4:
            return 0.15
        if odd >= 2.4:
            return 0.2
    
    def __getSuccessRateMessage_20240122(self, match :Match, ht_line, ht_odd:float, ft_line, ft_odd:float) -> str:
        previous_records = self.repository.GetResults(True)
        win_rate = "\n"
        match_goalline_records = [x for x in previous_records if x.ht_prematch_goalline == ht_line and x.ft_prematch_goalline == ft_line]
        
        success = 0
        two_ball_success = 0
        total = 0
        self.logger.debug(f"[{match.id}]將檢查全場半場賠率, 時間值:{match.time_int}, 半場{ht_line}賠率值:{ht_odd}, 全場{ft_line}賠率值:{ft_odd}")
        
        ht_odd_increment = self.__getOddIncrement(ht_odd)
        ft_odd_increment = self.__getOddIncrement(ft_odd)
        self.logger.debug(f"[{match.id}]半場誤差值{ht_odd_increment}, 全場誤差值{ft_odd_increment}")
        
        if match.is_first_half:
            for record in match_goalline_records:
                if (ht_odd <= record.ht_prematch_odd + ht_odd_increment and
                    ht_odd >= record.ht_prematch_odd - ht_odd_increment and
                    ft_odd <= record.ft_prematch_odd + ft_odd_increment and
                    ft_odd >= record.ft_prematch_odd - ft_odd_increment and
                    match.time_int == record.ht_time):
                    if record.ht_success > 0:
                        success += 1
                    if record.ht_success > 1:
                        two_ball_success += 1
                        
                    total += 1
            self.logger.debug(f"[{match.id}]檢查結果: 共有{total}類似紀錄, 共{success}場成功賽事")
        
        else:
            for record in match_goalline_records:
                if (ht_odd <= record.ht_prematch_odd + ht_odd_increment and
                    ht_odd >= record.ht_prematch_odd - ht_odd_increment and
                    ft_odd <= record.ft_prematch_odd + ft_odd_increment and
                    ft_odd >= record.ft_prematch_odd - ft_odd_increment and
                    match.time_int == record.ft_time):
                    if record.ft_success > 0:
                        success += 1
                    if record.ft_success > 1:
                        two_ball_success += 1
                    total += 1
            self.logger.debug(f"[{match.id}]檢查結果: 共有{total}類似紀錄, 共{success}場成功賽事")
            
        if total < 4:
            return ""
        
        success_rate = (success/total) * 100
        two_ball_success_rate = (two_ball_success/total) * 100
        win_rate += f"賽前{'半' if match.is_first_half else '全'}場中位數: {ht_line if match.is_first_half else ft_line}\n"
        win_rate += f"通知發放時間於{match.time_text}"
        win_rate += f"\n半場大波賠率{ht_odd}±{ht_odd_increment}"
        win_rate += f"\n全場大波賠率{ft_odd}±{ft_odd_increment}"
        win_rate += f"\n相類似{total}場賽事有1球機會: {success_rate:.2f}%"
        win_rate += f"\n有2球機會: {two_ball_success_rate:.2f}%"
        
        dto = self.repository.GetResultById(match.id)
        if dto is not None:
            if match.is_first_half:
                dto.ht_prob = success_rate
            else:
                dto.ft_prob = success_rate
            self.repository.Upsert(dto)
            
        recent_days = 3
        self.logger.debug(f"檢查最近{recent_days}日已紀錄場次可靠程度")
        recent_matches = [x for x in previous_records if x.match_date is not None and (match.date.date() - x.match_date.date()).days <= recent_days]
        
        reliable = 0
        reliable_total = 0
        for recent_match in recent_matches:
            if recent_match.ht_prob is not None:
                if (recent_match.ht_prob > 50 and recent_match.ht_success) or (recent_match.ht_prob < 50 and not recent_match.ht_success):
                    reliable += 1
                if recent_match.ht_prob != 50:
                    reliable_total += 1
            if recent_match.ft_prob is not None:
                if (recent_match.ft_prob > 50 and recent_match.ft_success) or (recent_match.ft_prob < 50 and not recent_match.ft_success):
                    reliable += 1
                if recent_match.ft_prob != 50:
                    reliable_total += 1
                
        self.logger.debug(f"共有{reliable_total}場有效場次, 可靠場次有{reliable}場")
        
        if reliable_total < 4:
            return win_rate
                
        reliable_rate = (reliable / reliable_total) * 100
        self.logger.debug(f"近{recent_days}日牙bot預測可靠程度:{reliable_rate}%")
        win_rate += f"\n近{recent_days}日牙bot預測可靠程度:{reliable_rate}%"
            
        return win_rate
    
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
                    last_min_dto = self.repository.GetResultById(m.id)
                    if last_min_dto is not None:
                        last_min_dto.ht_last_min = True
                        self.repository.Upsert(last_min_dto)
                    self.ht_last_min.append(m.id)
                    if len(self.ht_last_min) > 5:
                        _ = self.ht_last_min.pop(0)
                        
                if m.time_int >= 86 and not m.is_first_half and m.id not in self.ft_last_min:
                    header = f'{m.home_name} 對 {m.away_name} 全場最後4分鐘'
                    body = f'目前球賽時間 {m.time_text}'
                    toReturn.append((header, body))
                    last_min_dto = self.repository.GetResultById(m.id)
                    if last_min_dto is not None:
                        last_min_dto.ft_last_min = True
                        self.repository.Upsert(last_min_dto)
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
                prematch_odds = self.crawler.GetPreMatchOdds(m.id)
                ht_prematch_goal_line = list(prematch_odds['ht'])[0]
                self.logger.debug(f"{m.id}賽事賽前半場中位數 {ht_prematch_goal_line}")
                ft_prematch_goal_line = list(prematch_odds['ft'])[0]
                self.logger.debug(f"{m.id}賽事賽前全場中位數 {ft_prematch_goal_line}")
                ht_prematch_high_odd = prematch_odds['ht'][ht_prematch_goal_line][0]
                ht_prematch_odd_flow = prematch_odds['ht'][ht_prematch_goal_line][1]
                self.logger.debug(f"{m.id}賽事賽前半場大波賠率 {ht_prematch_high_odd}")
                ft_prematch_high_odd = prematch_odds['ft'][ft_prematch_goal_line][0]
                ft_prematch_odd_flow = prematch_odds['ft'][ft_prematch_goal_line][1]
                self.logger.debug(f"{m.id}賽事賽前全場大波賠率 {ft_prematch_high_odd}")
                if m.is_first_half:
                    if ht_prematch_odd_flow is None:
                        flow = '無升跌'
                    elif ht_prematch_odd_flow:
                        flow = '回飛'
                    else:
                        flow = '落飛'
                else:
                    if ft_prematch_odd_flow is None:
                        flow = '無升跌'
                    elif ft_prematch_odd_flow:
                        flow = '回飛'
                    else:
                        flow = '落飛'
                self.logger.debug(f"{m.id}賽事賽前大波賠率為 {flow}")
                
                dto = self.repository.GetResultById(m.id)
                if dto is None and m.is_first_half:
                    self.logger.debug(f"{m.id}賽事為新增項目, 將新增至資料庫")
                    new_dto = ResultDto(m.id, m.time_int, odd)
                    new_dto.match_date = m.date
                    self.repository.Upsert(new_dto)
                elif not dto is None and not m.is_first_half:
                    self.logger.debug(f"{m.id}為下半場賽事, 將更新資料庫")
                    dto.ft_time = m.time_int
                    dto.ft_odd = odd
                    self.repository.Upsert(dto)
                            
                header = f'{m.home_name} 對 {m.away_name} 即場0.75大有水'
                body = f'目前球賽時間 {m.time_text}\n'
                body += f'目前賠率: 0.5/1.0大 - {odd}\n'
                body += f'賽前賠率: {ht_prematch_goal_line if m.is_first_half else ft_prematch_goal_line}大 - {ht_prematch_high_odd if m.is_first_half else ft_prematch_high_odd}, {flow}\n'
                body += f'{self.__getSuccessRateMessage_20240122(m, ht_prematch_goal_line, ht_prematch_high_odd, ft_prematch_goal_line, ft_prematch_high_odd)}'
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
    fetcher = Fetcher(CONNECTION_STRING, loggerFact)
    fetcher.FillMatchResults()
    
    #result = crawler.GetWebsiteData("http://g10oal.com", "/live").text
    #soup = BeautifulSoup(result, "html.parser")
    #matches = soup.find_all(class_="card-body")
    #m = Match(matches[0])
    #print('主隊 ' + m.home_name)
    #print('客隊 '+ m.away_name)
    #print('Id ' + m.id)
    #print('時間(int) ' + str(m.time_int))
    #print('時間 ' + m.time_text)
    #print('開咗場 ' + str(m.is_started))
    #print('入咗波 ' + str(m.is_goaled))
    #print('有即場 ' + str(m.is_live_match))
    #print('上半場 ' + str(m.is_first_half))
    #print('日期' + str(m.date))