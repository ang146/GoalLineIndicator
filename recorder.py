import pyodbc

class Recorder:
    conn = None
    cursor = None
    
    def __init__(self, connection_string:str):
        self.conn = pyodbc.connect(connection_string)
        self.cursor = self.conn.cursor()
    
    def CheckIsNew(self, match_id) -> bool:
        result = self.cursor.execute("select match_id from [HKJC_Odds].[dbo].[half_time_live] where match_id = ?", match_id).fetchone()
        return result is None
    
    def WriteMatch(self, match_id:str, recorded_time:int, recorded_odd:float, before_match_odd:float):
        is_new = self.CheckIsNew(match_id)
        data = [(match_id, recorded_time, recorded_odd, before_match_odd)]
        if is_new:
            self.cursor.executemany("insert into [HKJC_Odds].[dbo].[half_time_live] (match_id, recorded_time, recorded_odd, before_match_odd) values (?, ?, ?, ?)", data)
            self.cursor.commit()
    
    def WriteResult(self, match_id:str, success: bool):
        is_new = self.CheckIsNew(match_id)
        if success:
            data = (1, match_id)
        else:
            data = (0, match_id)
        if not is_new:
            self.cursor.execute("update [HKJC_Odds].[dbo].[half_time_live] set success = ? where match_id = ?", data)
            self.cursor.commit()
            
    def GetSuccessRateByTime(self, match_time_min :int, match_time_max :int) -> float:
        query = "select success from [HKJC_Odds].[dbo].[half_time_live] where recorded_time <= ? and recorded_time >= ?"
        results = self.cursor.execute(query, (match_time_max, match_time_min)).fetchall()
        if len(results) == 0:
            return -1
        
        succeed = 0
        total = 0
        for result in results:
            if not result.success is None:
                total += 1
                if result.success:
                    succeed += 1
        if total < 10:
            return -1
        
        return succeed/total * 100
                
if __name__ == "__main__":
    from config import *
    from bs4 import BeautifulSoup
    import requests
    driver = 'SQL SERVER'
    connection_string = f"""
        DRIVER={{{driver}}};
        SERVER={SERVER_NAME};
        DATABASE={DATABASE_NAME};
        PORT=49172;
        Trust_Connection=yes;
        uid={USER_NAME};
        pwd={PASSWORD};
    """    
    def GetWebsiteData(site_domain :str, site_api :str) -> requests.Response:
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
    def FindGoalLineOdds(match_odd_url :str, odd_type:str) -> dict:
        result = GetWebsiteData("http://g10oal.com", match_odd_url).text
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
    
    conn = pyodbc.connect(connection_string)
    cursor = conn.cursor()
    match_ids = cursor.execute("select match_id from [HKJC_Odds].[dbo].[half_time_live]").fetchall()
    for id_list in match_ids:
        id = id_list[0]
        odds = FindGoalLineOdds(f"/match/{id}/odds", 'fhl')
        one_five = odds['1.5']
        cursor.execute("update [HKJC_Odds].[dbo].[half_time_live] set before_match_odd = ? where match_id = ?", (one_five, id))
        cursor.commit()
    
    #recorder = Recorder(connection_string)
    #print(recorder.GetSuccessRateByTime(13,16))