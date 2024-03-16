from enum import Enum
import requests
import json
import bs4
import Utils
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
            to_return['ht'] = Utils.GetGoals(ht_score)
            to_return['ft'] = Utils.GetGoals(ft_score)
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
    HKJC_All_Odd_Api = '/football/getJSON.aspx?jsontype=odds_allodds.aspx'
    HKJC_Result_Api = '/football/getJSON.aspx?jsontype=results.aspx'

if __name__ == "__main__":
    from LoggerFactory import LoggerFactory
    loggerFact = LoggerFactory()
    crawler = Crawler(loggerFact)

    result = crawler.GetWebsiteData(SiteApi.HKJC.value, SiteApi.HKJC_Result_Api.value).json()

