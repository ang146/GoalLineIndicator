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
                prematch_odds = self.crawler.GetPreMatchOdds(dto.id)
                ht_goalline = list(prematch_odds['ht'])[0]
                dto.ht_prematch_goalline = ht_goalline
                dto.ht_prematch_odd = prematch_odds['ht'][ht_goalline][0]
                dto.ht_rise = prematch_odds['ht'][ht_goalline][1]
                dto.ht_success = scores['ht'] != 0
                self.logger.debug(f"已取得{dto.id}賽事半場賽果: 半場入球{scores['ht']}, 中位數入球{ht_goalline}, 入球大賠率{dto.ht_prematch_odd}, 賠率流向為上升{dto.ht_rise}")
                    
                ft_goalline = list(prematch_odds['ft'])[0]
                dto.ft_prematch_goalline = ft_goalline
                dto.ft_prematch_odd = prematch_odds['ft'][ft_goalline][0]
                dto.ft_rise = prematch_odds['ft'][ft_goalline][1]
                dto.ft_success = scores['ft'] != 0
                self.logger.debug(f"已取得{dto.id}賽事全場賽果: 全半場入球{scores['ht']}, 中位數入球{ft_goalline}, 入球大賠率{dto.ft_prematch_odd}, 賠率流向為上升{dto.ft_rise}")
                    
                self.repository.Upsert(dto)

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
            self.logger.info("沒有任何賽事數據, 將閒置Thread 20分鐘")
            time.sleep(1200)
        else:
            if not any(not x.is_started for x in matches) and (all(not x.is_live_match for x in matches) or all(x.is_goaled for x in matches) or all(not x.is_live_match or x.is_goaled for x in matches)):
                self.logger.info("所有賽事均無即場或已入球, 將閒置Thread 30分鐘")
                time.sleep(1800)
            elif all(not x.is_started for x in matches) or all(not x.is_live_match or x.is_goaled or not x.is_started for x in matches):
                self.logger.info("所有賽事均未開賽, 或已開賽但沒有即場或已入球, 將閒置Thread 1分鐘")
                time.sleep(60)
        
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
                    self.repository.Upsert(new_dto)
                elif not dto is None and not m.is_first_half:
                    self.logger.debug(f"{m.id}為下半場賽事, 將更新資料庫")
                    dto.ft_time = m.time_int
                    dto.ft_odd = odd
                    self.repository.Upsert(dto)
                
                def GetSuccessRateMessage() -> str:
                    previous_records = self.repository.GetResults(True)
                    
                    match_goalline_records = [x for x in previous_records if x.ht_prematch_goalline == ht_prematch_goal_line and x.ft_prematch_goalline == ft_prematch_goal_line]
                    win_rate_message = "\n資料庫相同半場及全場中位數的成功率:\n"
                    htft_check = True
                    
                    if len(match_goalline_records) < 10:
                        htft_check = False
                        if m.is_first_half:
                            match_goalline_records = [x for x in previous_records if x.ht_prematch_goalline == ht_prematch_goal_line]
                            win_rate_message = "\n資料庫相同半場中位數的成功率:\n"
                        else:
                            match_goalline_records = [x for x in previous_records if x.ft_prematch_goalline == ft_prematch_goal_line]
                            win_rate_message = "\n資料庫相同全場中位數的成功率:\n"
                    
                    if len(match_goalline_records) < 10:
                        return ""
                    
                    def RecursiveWinRate(win_rate :str, time_increment:int = 0, ht_odd_increment:float = 0.0, ft_odd_increment:float = 0.0) -> (int, str):
                        success = 0
                        total = 0
                        ht_odd_increment = round(ht_odd_increment, 2)
                        ft_odd_increment = round(ft_odd_increment, 2)
                        
                        if htft_check:
                            self.logger.debug(f"[{m.id}]將檢查全場半場賠率, 時間值:{time_increment}, 半場賠率值:{ht_odd_increment}, 全場賠率值:{ft_odd_increment}")
                            if m.is_first_half:
                                for record in match_goalline_records:
                                    if (ht_prematch_high_odd <= record.ht_prematch_odd + ht_odd_increment and
                                        ht_prematch_high_odd >= record.ht_prematch_odd - ht_odd_increment and
                                        ft_prematch_high_odd <= record.ft_prematch_odd + ft_odd_increment and
                                        ft_prematch_high_odd >= record.ft_prematch_odd - ft_odd_increment and
                                        m.time_int <= record.ht_time + time_increment and
                                        m.time_int >= record.ht_time - time_increment):
                                        if record.ht_success:
                                            success += 1
                                        total += 1
                                if total < 10:
                                    if time_increment < 2:
                                        return RecursiveWinRate(win_rate, time_increment + 1, ht_odd_increment, ft_odd_increment)
                                    if ft_odd_increment < 0.09:
                                        return RecursiveWinRate(win_rate, 0, ht_odd_increment, ft_odd_increment + 0.01)
                                    if ht_odd_increment < 0.09:
                                        return RecursiveWinRate(win_rate, 0, ht_odd_increment + 0.01, 0)
                                    return (-2, win_rate)
                                
                                success_rate = (success/total) * 100
                                win_rate += f"賽前全場中位數: {ft_prematch_goal_line}\n"
                                win_rate += f"通知發放時間於" + (f"{m.time_text}" if time_increment == 0 else f"{m.time_int - time_increment if m.time_int - time_increment >= 0 else 0}'至{m.time_int + time_increment}'")
                                win_rate += "\n半場大波賠率" + (f"{ht_prematch_high_odd}" if ht_odd_increment == 0 else f"{ht_prematch_high_odd}±{ht_odd_increment}")
                                win_rate += "\n全場大波賠率" + (f"{ft_prematch_high_odd}" if ft_odd_increment == 0 else f"{ft_prematch_high_odd}±{ft_odd_increment}")
                                win_rate += f"\n成功率: {success_rate:.2f}%"
                                
                                return (1, win_rate)
                            else:
                                for record in match_goalline_records:
                                    if (ht_prematch_high_odd <= record.ht_prematch_odd + ht_odd_increment and
                                        ht_prematch_high_odd >= record.ht_prematch_odd - ht_odd_increment and
                                        ft_prematch_high_odd <= record.ft_prematch_odd + ft_odd_increment and
                                        ft_prematch_high_odd >= record.ft_prematch_odd - ft_odd_increment and
                                        m.time_int <= record.ht_time + time_increment and
                                        m.time_int >= record.ht_time - time_increment):
                                        if record.ft_success:
                                            success += 1
                                        total += 1
                                if total < 10:
                                    if time_increment < 2:
                                        return RecursiveWinRate(win_rate, time_increment + 1, ht_odd_increment, ft_odd_increment)
                                    if ht_odd_increment < 0.09:
                                        return RecursiveWinRate(win_rate, 0, ht_odd_increment + 0.01, ft_odd_increment)
                                    if ft_odd_increment < 0.09:
                                        return RecursiveWinRate(win_rate, 0, 0, ft_odd_increment + 0.01)
                                    return (-2, win_rate)
                                
                                success_rate = (success/total) * 100
                                win_rate += f"賽前半場中位數: {ht_prematch_goal_line}\n"
                                win_rate += f"通知發放時間於" + (f"{m.time_text}" if time_increment == 0 else f"{m.time_int - time_increment if m.time_int - time_increment >= 0 else 0}'至{m.time_int + time_increment}'")
                                win_rate += "\n半場大波賠率" + (f"{ht_prematch_high_odd}" if ht_odd_increment == 0 else f"{ht_prematch_high_odd}±{ht_odd_increment}")
                                win_rate += "\n全場大波賠率" + (f"{ft_prematch_high_odd}" if ft_odd_increment == 0 else f"{ft_prematch_high_odd}±{ft_odd_increment}")
                                win_rate += f"\n成功率: {success_rate:.2f}%"
                                
                                return (1, win_rate)

                        else:
                            if m.is_first_half:
                                self.logger.debug(f"[{m.id}]將檢查半場賠率, 時間值:{time_increment}, 半場賠率值:{ht_odd_increment}, 全場賠率值:{ft_odd_increment}")
                                for record in match_goalline_records:
                                    if (ht_prematch_high_odd <= record.ht_prematch_odd + ht_odd_increment and 
                                        ht_prematch_high_odd >= record.ht_prematch_odd - ht_odd_increment and 
                                        m.time_int <= record.ht_time + time_increment and
                                        m.time_int >= record.ht_time - time_increment):
                                        if record.ht_success:
                                            success += 1
                                        total += 1
                                if total < 10:
                                    if time_increment < 2:
                                        return RecursiveWinRate(win_rate, time_increment + 1, ht_odd_increment, 0)
                                    if ht_odd_increment < 0.09:
                                        return RecursiveWinRate(win_rate, 0, ht_odd_increment + 0.01, 0)
                                    return (-1, win_rate)
                            
                                success_rate = (success/total) * 100
                                win_rate += f"通知發放時間於" + (f"{m.time_text}" if time_increment == 0 else f"{m.time_int - time_increment if m.time_int - time_increment >= 0 else 0}'至{m.time_int + time_increment}'")
                                win_rate += "\n半場大波賠率" + (f"{ht_prematch_high_odd}" if ht_odd_increment == 0 else f"{ht_prematch_high_odd}±{ht_odd_increment}")
                                win_rate += f"\n成功率: {success_rate:.2f}%"
                                
                                return (1, win_rate)
                            else:
                                self.logger.debug(f"[{m.id}]將檢查全場賠率, 時間值:{time_increment}, 半場賠率值:{ht_odd_increment}, 全場賠率值:{ft_odd_increment}")
                                for record in match_goalline_records:
                                    if (record.ft_time is None):
                                        continue
                                    if (ft_prematch_high_odd <= record.ft_prematch_odd + ft_odd_increment and 
                                        ft_prematch_high_odd >= record.ft_prematch_odd - ft_odd_increment and 
                                        m.time_int <= record.ft_time + time_increment and
                                        m.time_int >= record.ft_time - time_increment):
                                        if record.ft_success:
                                            success += 1
                                        total += 1
                                if total < 10:
                                    if time_increment < 2:
                                        return RecursiveWinRate(win_rate, time_increment + 1, 0, ft_odd_increment)
                                    if ft_odd_increment < 0.09:
                                        return RecursiveWinRate(win_rate, 0, 0, ft_odd_increment + 0.01)
                                    return (-1, win_rate)
                            
                                success_rate = (success/total) * 100
                                win_rate += f"通知發放時間於" + (f"{m.time_text}" if time_increment == 0 else f"{m.time_int - time_increment if m.time_int - time_increment >= 0 else 0}'至{m.time_int + time_increment}'")
                                win_rate += "\n全場大波賠率" + (f"{ft_prematch_high_odd}" if ft_odd_increment == 0 else f"{ft_prematch_high_odd}±{ft_odd_increment}")
                                win_rate += f"\n成功率: {success_rate:.2f}%"
                                
                                return (1, win_rate)
                    
                    result = RecursiveWinRate(win_rate_message)
                    if result[0] == -2:
                        htft_check = False
                        if m.is_first_half:
                            match_goalline_records = [x for x in previous_records if x.ht_prematch_goalline == ht_prematch_goal_line]
                            win_rate_message = "資料庫相同半場中位數的成功率:\n"
                        else:
                            match_goalline_records = [x for x in previous_records if x.ft_prematch_goalline == ft_prematch_goal_line]
                            win_rate_message = "資料庫相同全場中位數的成功率:\n"
                        result = RecursiveWinRate(win_rate_message)
                        
                    if result[0] == -1:
                        return ""
                        
                    return result[1]

                header = f'{m.home_name} 對 {m.away_name} 即場0.75大有水'
                body = f'目前球賽時間 {m.time_text}\n'
                body += f'目前賠率: 0.5/1.0大 - {odd}\n'
                body += f'賽前賠率: {ht_prematch_goal_line if m.is_first_half else ft_prematch_goal_line}大 - {ht_prematch_high_odd if m.is_first_half else ft_prematch_high_odd}, {flow}\n'
                body += f'{GetSuccessRateMessage()}'
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