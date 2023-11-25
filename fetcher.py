from bs4 import BeautifulSoup
import requests
from config import *
import json
from typing import Tuple, List
from recorder import Recorder

G10OALSITE = 'http://g10oal.com'
HKJC_SITE = 'http://bet.hkjc.com'
FULLTIME_GOALLINE="hil"
HALFTIME_GOALLINE="fhl"
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
        
    def __GetWebsiteData(self, site_domain :str, site_api :str) -> requests.Response:
        session = requests.Session()
        r = session.get(site_domain)
        
        header = {"referer": site_domain, 
            "user-agent": 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.111 Safari/537.36'}

        liveMatches_url = f'{site_domain}{site_api}'
        result = session.post(
            liveMatches_url,
            headers=header,
            cookies=r.cookies
        )
        return result
    
    def __FetchHalfTimeOdds(self, match_id :str, is_live_match) -> Tuple[str, float]:
        no_result = ('', 0)
        if not is_live_match:
            print(f'場次{match_id}無即場, 跳過即場半場檢查')
            return no_result
        
        if match_id in self.half_time_fetch_cache:
            print(f'場次{match_id}已通知, 將跳過')
            return no_result
        
        hkjc_domain = HKJC_SITE
        hkjc_api = f'/football/getJSON.aspx?jsontype=odds_allodds.aspx&matchid={match_id}'
        
        result = self.__GetWebsiteData(hkjc_domain, hkjc_api)

        e = True
        trial = 1
        while e:
            try:
                match_response = result.json()
                try:
                    match_data = next(item for item in match_response['matches'] if item["matchID"] == match_id)
                except:
                    return no_result
                if 'fhlodds' in match_data:
                    print(f'賽事{match_id}有即場, 將提取是次賽事半大賠率')
                    for line in match_data['fhlodds']['LINELIST']:
                        if line['LINE'] != '0.5/1.0':
                            continue
                        odd = float(line['H'][4:])
                        print(f'場次{match_id}半大賠率為{odd}')
                        if odd >= 2:
                            return (line['LINE'], odd)
                        else:
                            break
                e = False
            except ConnectionError:
                print("Current connection is not available, aborting process. ")
                e = False
                break
            except json.decoder.JSONDecodeError:
                if trial > 50:
                    print(f"Match {match_id} was unable to retrieve. Giving up.")
                    e = False
                    break
                
                e = True
                trial += 1
                result = self.__GetWebsiteData(hkjc_domain, hkjc_api)
        return no_result
        
    def __FindGoalLineOdds(self, match_odd_url :str, odd_type:str) -> dict:
        result = self.__GetWebsiteData(G10OALSITE, match_odd_url).text
        soup = BeautifulSoup(result, "html.parser")
        table = soup.find("a", {"name" : odd_type}).next_sibling.next_sibling.next_sibling.next_sibling
        tbody = table.find("tbody")
        odds = tbody.find_all("tr", class_=lambda c: c != "table-secondary")
        wanted_odds = {}
        for odd in odds:
            goalLines = odd.find_all("td", class_="text-center")
            goalLine = goalLines[1].text
            
            highOdd = float(goalLines[0].text)
            
            if goalLine in wanted_odds.keys():
                continue
            wanted_odds[goalLine] = highOdd
        return wanted_odds

    def FindMatch(self) -> List[List[str]]:
        print(f"進行第{self.fetch_counter}次fetching")
        result = self.__GetWebsiteData(G10OALSITE, "/live").text
        soup = BeautifulSoup(result, "html.parser")
        matches = soup.find_all(class_="card-body")
        toReturn = []
        
        for match in matches:
            centerText = match.select_one(".text-center")
            home_name = Fetcher.TrimString(match.find("div", class_=lambda c: "text-right" in c).text)
            away_name = Fetcher.TrimString(match.find("div", class_=lambda c: "text-left" in c).text)
            
            if "未開賽" in centerText.text:
                print(f"{home_name} 對 {away_name}未開賽, 將跳過")
                continue
            
            currentIntTime = 0
            match_time = None
            match_score = centerText.find("div", class_="lead").text
            matchOddsLink = match.find("a", href=True)['href'][0:-7] + "odds"
            match_id = matchOddsLink[-13:-5]
            match_url = f"https://bet.hkjc.com/football/odds/odds_inplay_all.aspx?lang=CH&tmatchid={match_id}"
            
            half_time_score = centerText.find("small")
            if not half_time_score is None and self.use_db:
                if half_time_score.text == "(0-0)":
                    self.recorder.WriteResult(match_id, False)
                else:
                    self.recorder.WriteResult(match_id, True)
            
            if match_score != "0-0":
                print(f"{home_name} 對 {away_name}目前比分並非0-0, 將跳過")
                continue
            
            currentTime = centerText.select_one(".text-danger").text
            if not currentTime.startswith("半場"):
                match_time = currentTime[3:]
                if len(currentTime) > 6:
                    if (currentTime[3:].startswith("4") or currentTime[3].startswith("5")):
                        currentIntTime = 45
                    else:
                        currentIntTime = 90
                else:
                    currentIntTime = int(currentTime[3:-1])
            else:
                match_time = "半場"
                currentIntTime = 46
            
            
            if currentIntTime <= 45:
                is_live_match = match.find('span', class_=lambda c: "badge-danger" in c)
                half_time_odds = self.__FetchHalfTimeOdds(match_id, is_live_match)
                if half_time_odds[0]:
                    win_rate = ""
                    half_time_odds_before_match = self.__FindGoalLineOdds(matchOddsLink, HALFTIME_GOALLINE)
                    one_five_odd = half_time_odds_before_match["1.5"]
                    if self.use_db:
                        self.recorder.WriteMatch(match_id, currentIntTime, half_time_odds[1], one_five_odd)
                        exact_win_rate = self.recorder.GetSuccessRateByTime(currentIntTime, currentIntTime)
                        min_minutes = int(currentIntTime / 5) * 5 + 1
                        group_mins_win_rate = self.recorder.GetSuccessRateByTime(min_minutes, min_minutes + 4)
                        range_mins_win_rate = self.recorder.GetSuccessRateByTime(currentIntTime-2, currentIntTime+2)
                        if exact_win_rate != -1:
                            win_rate += "\n提醒於{time}分發出的成功率為: {rate:.2f}".format(time= currentIntTime,rate=exact_win_rate)
                        if group_mins_win_rate != -1:
                            win_rate += "\n提醒於{time_min}分至{time_max}分發出的成功率為: {rate:.2f}".format(time_min= min_minutes, time_max= min_minutes + 4, rate=group_mins_win_rate)
                        if range_mins_win_rate != -1:
                            win_rate += "\n提醒於{time_min}分至{time_max}分發出的成功率為: {rate:.2f}".format(time_min= currentIntTime -2, time_max= currentIntTime + 2, rate=range_mins_win_rate)
                        
                    header = f'{home_name} 對 {away_name} 即場半場0.75大有水'
                    body = f'目前球賽時間 {match_time}, 目前賠率:\n{half_time_odds[0]}大: {half_time_odds[1]} {win_rate}'
                    toReturn.append([header, body, match_url])
                    
                    if len(self.half_time_fetch_cache) > 100:
                        _ = self.half_time_fetch_cache.pop(0)
                        
                    self.half_time_fetch_cache.append(match_id)
                    continue
                    
            if currentIntTime < TOCHECKMINUTES:
                print(f"{home_name} 對 {away_name}目前未過{TOCHECKMINUTES}分鐘, 將跳過")
                continue
                
            if match_id in self.full_time_fetch_cache and not self.keep_notifying:
                print(f"{home_name} 對 {away_name} 已經出咗通知, 唔會再出")
                continue
            
            goalLines = self.__FindGoalLineOdds(matchOddsLink, FULLTIME_GOALLINE)
            match_title = home_name + " 對 " + away_name + " 大波有feel"
            match_body = f"目前球賽時間 {match_time}, 賽前賠率:\n"
            goal_line_odds_message = ""
            for line in goalLines:
                if goalLines[line] <= DESIREDHIGHODD:
                    goal_line_odds_message += f"{line}大 : {goalLines[line]}倍\n"
            match_body += goal_line_odds_message
            if len(goal_line_odds_message) > 0:
                print(f"~~{home_name} 對 {away_name} 符合要求~~")
                toReturn.append([match_title, match_body, match_url])
                
                if len(self.full_time_fetch_cache) > 50:
                    _ = self.full_time_fetch_cache.pop(0)
                    
                self.full_time_fetch_cache.append(match_id)
                
            else:
                print(f"{home_name} 對 {away_name}大小球賠率不符合自訂要求, 將跳過")
                continue
        self.fetch_counter += 1
        return toReturn
    
    @staticmethod
    def TrimString(toTrim :str):
        trimmed = toTrim.replace("1", "").replace("2", "").replace("3", "").replace("4", "").replace("5", "").replace("6", "").replace("7", "").strip()
        return trimmed