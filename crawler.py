from enum import Enum
import requests
import json
import bs4
import utils
import time

class Crawler:
    def __init__(self, loggerFactory):
        self.logger = loggerFactory.getLogger("Crawler")
    
    def GetWebsiteData(self, site_domain :str, site_api :str) -> requests.Response:
        try:
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
        except ConnectionError:
            print("Connection error, retrying.")
            time.sleep(1)
            return self.GetWebsiteData(site_domain, site_api)

    def GetMatchResults(self, match_id:str) -> dict:
        result = self.GetWebsiteData(SiteApi.G10OAL.value, SiteApi.G10OAL_Odd_Api.value.format(match_id)).text
        soup = bs4.BeautifulSoup(result, "html.parser")
        score_board = soup.findAll('div', class_=lambda c: c == 'text-center')[1]
        to_return = {}
        if '場已完' in score_board.text or 'FT 90' in score_board.text:
            ft_score :str = score_board.find('div', class_='lead').text
            ht_score :str = score_board.find('div', class_='text-muted').find('small').text
            to_return['ht'] = utils.get_goals(ht_score)
            to_return['ft'] = utils.get_goals(ft_score)
        return to_return
        
    def GetPreMatchOdds(self, match_id:str) -> dict:
        result = self.GetWebsiteData(SiteApi.G10OAL.value, SiteApi.G10OAL_Odd_Api.value.format(match_id)).text
        soup = bs4.BeautifulSoup(result, "html.parser")
        
        wanted_odds = {'ht':{}, 'ft':{}}
        fhl_table = soup.find("a", {"name" : 'fhl'}).next_sibling.next_sibling.next_sibling.next_sibling
        fhl_tbody = fhl_table.find("tbody")
        fhl_odds = fhl_tbody.find_all("tr", class_=lambda c: c != "table-secondary")
        check_median = 999.0
        for odd in fhl_odds:
            goalLines = odd.find_all("td", class_="text-center")
            goalLine = goalLines[1].text
            
            highOdd = float(goalLines[0].text)
            
            if goalLine in wanted_odds['ht'].keys():
                if wanted_odds['ht'][goalLine][0] > highOdd:
                    wanted_odds['ht'][goalLine] = (wanted_odds['ht'][goalLine][0], True)
                elif wanted_odds['ht'][goalLine][0] < highOdd: 
                    wanted_odds['ht'][goalLine] = (wanted_odds['ht'][goalLine][0], False)
                continue
            
            lowOdd = float(goalLines[2].text)
            median = abs(highOdd - lowOdd)
            if median < check_median:
                check_median = median
                wanted_odds['ht'].clear()
                wanted_odds['ht'][goalLine] = (highOdd, None)
        
        hil_table = soup.find("a", {"name" : 'hil'}).next_sibling.next_sibling.next_sibling.next_sibling
        hil_tbody = hil_table.find("tbody")
        hil_odds = hil_tbody.find_all("tr", class_=lambda c: c != "table-secondary")
        check_median = 999.0
        for odd in hil_odds:
            goalLines = odd.find_all("td", class_="text-center")
            goalLine = goalLines[1].text
            
            highOdd = float(goalLines[0].text)
            
            if goalLine in wanted_odds['ft'].keys():
                if wanted_odds['ft'][goalLine][0] > highOdd:
                    wanted_odds['ft'][goalLine] = (wanted_odds['ft'][goalLine][0], True)
                elif wanted_odds['ft'][goalLine][0] < highOdd:
                    wanted_odds['ft'][goalLine] = (wanted_odds['ft'][goalLine][0], False)
                continue
            
            lowOdd = float(goalLines[2].text)
            median = abs(highOdd - lowOdd)
            if median < check_median:
                check_median = median
                wanted_odds['ft'].clear()
                wanted_odds['ft'][goalLine] = (highOdd, None)
                    
        return wanted_odds
        
    def GetLiveTimeOdds(self, match_id:str) -> float:
        result = self.GetWebsiteData(SiteApi.HKJC.value, SiteApi.HKJC_Odd_Api.value.format(match_id))
        
        e = True
        trial = 1
        while e:
            try:
                match_response = result.json()
                try:
                    match_data = next(item for item in match_response['matches'] if item["matchID"] == match_id)
                except:
                    return
                if 'fhlodds' in match_data:
                    for line in match_data['fhlodds']['LINELIST']:
                        if line['LINE'] != '0.5/1.0':
                            continue
                        odd = float(line['H'][4:])
                        return odd
                if 'hilodds' in match_data:
                    for line in match_data['hilodds']['LINELIST']:
                        if line['LINE'] != '0.5/1.0':
                            continue
                        odd = float(line['H'][4:])
                        return odd
                
                e = False
            except ConnectionError:
                print("Current connection is not available, aborting process. ")
                e = False
                break
            except json.decoder.JSONDecodeError:
                if trial > 20:
                    print(f"Match {match_id} was unable to retrieve. Giving up.")
                    e = False
                    break
                
                e = True
                trial += 1
                result = self.GetWebsiteData(SiteApi.HKJC.value, SiteApi.HKJC_Odd_Api.value.format(match_id))
        return -1

class SiteApi(Enum):
    G10OAL = 'http://g10oal.com'
    G10OAL_Live_Mathces = '/live'
    G10OAL_Odd_Api = '/match/{0}/odds'
    HKJC = 'http://bet.hkjc.com'
    HKJC_Odd_Api = '/football/getJSON.aspx?jsontype=odds_allodds.aspx&matchid={0}'
    HKJC_Result_Api = '/football/getJSON.aspx?jsontype=results.aspx'

from datetime import datetime

class Temp_Match:
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
    match_cache = {}
        
    def __init__(self, data, data_site:str):
        if data_site == SiteApi.HKJC.name:
            import dateutil.parser
            self.date = dateutil.parser.parse(data['matchDate'])
            self.is_live_match = True
            self.is_started = True
            self.home_name = data['homeTeam']['teamNameCH']
            self.away_name = data['awayTeam']['teamNameCH']
            self.id = data['matchID']
            if data['matchState'] == 'FirstHalf':
                self.is_first_half = True
            else:
                self.is_first_half = False

            if data['matchState'] == 'FirstHalf' or data['matchState'] == 'FirstHalfCompleted':
                if len(data['accumulatedscore']) == 0:
                    self.is_goaled = False
                elif data['accumulatedscore'][0]['home'] != '0' or data['accumulatedscore'][0]['away'] != '0':
                    self.is_goaled = True
            elif data['matchState'] == 'SecondHalf':
                if len(data['accumulatedscore']) == 0:
                    self.is_goaled = False
                elif data['accumulatedscore'][1]['home'] != '0' or data['accumulatedscore'][1]['away'] != '0':
                    self.is_goaled = True

            if self.is_goaled:
                return
            
            if data['matchState'] == 'FirstHalfCompleted' and self.id in self.match_cache:
                self.match_cache.pop(self.id, None)
                self.time_int = 46
                self.time_text = "半場"
            else:
                if not self.id in self.match_cache:
                    self.match_cache[self.id] = datetime.now()
                if data['matchState'] == 'FirstHalf':
                    self.time_int = int((datetime.now() - self.match_cache[self.id]).total_seconds() / 60)
                elif data['matchState'] == 'SecondHalf':
                    self.time_int = int((datetime.now() - self.match_cache[self.id]).total_seconds() / 60) + 46
                self.time_text = f"{self.time_int}'"
                    

            

        
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

if __name__ == "__main__":
    from LoggerFactory import LoggerFactory
    loggerFact = LoggerFactory()
    crawler = Crawler(loggerFact)

    result = crawler.GetWebsiteData(SiteApi.HKJC.value, SiteApi.HKJC_Result_Api.value).json()

    if len(result) < 2:
        print("Error")
        pass


    
    for match in (result[1]['matches'] + result[0]['matches']):
        available_state = ['FirstHalf', 'FirstHalfCompleted', 'SecondHalf']

        if not match['matchState'] in available_state:
            if match['awayTeam']['teamNameCH'] == '甘堡爾':
                print(f"{match['homeTeam']['teamNameCH']}對{match['awayTeam']['teamNameCH']} - {match['matchState']}")
            continue
        matchData = Temp_Match(match, SiteApi.HKJC.name)
        print(f'[{matchData.id}]{matchData.home_name}對{matchData.away_name}/上半場{matchData.is_first_half}/已入球{matchData.is_goaled}/有即場{matchData.is_live_match}/日期{matchData.date}/已開場{matchData.is_started}')
        if not matchData.time_int is None:
            print(f'比賽時間{matchData.time_int}')
        print(matchData.match_cache)

    
