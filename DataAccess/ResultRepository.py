import pyodbc
from typing import List
from .ResultDto import ResultDto

SELECT_QUERY = 'SELECT [id],[ht_time],[ht_odd],[ht_prematch_odd],[ht_prematch_goalline],[ft_time],[ft_odd],[ft_success],[ft_prematch_odd],[ft_prematch_goalline],[ht_rise],[ft_rise],[ht_success],[ht_last_min],[ft_last_min],[date],[ht_probability],[ft_probability],[ht_prediction] FROM [HKJC_Odds].[dbo].[live_match] '
UPDATE_QUERY = 'UPDATE [HKJC_Odds].[dbo].[live_match] set ht_time = ?, ht_odd = ?, ht_prematch_odd = ?, ht_prematch_goalline = ?, ht_rise = ?, ht_success = ?, ft_odd = ?, ft_prematch_odd = ?, ft_prematch_goalline = ?, ft_rise = ?, ft_success = ?, ft_time = ?, ht_last_min = ?, ft_last_min = ?, date = ?, ht_probability = ?, ft_probability = ?, ht_prediction = ? where id = ?'
INSERT_QUERY = 'INSERT INTO [HKJC_Odds].[dbo].[live_match] (ht_time, ht_odd, ht_prematch_odd, ht_prematch_goalline, ht_rise, ht_success, ft_odd, ft_prematch_odd, ft_prematch_goalline, ft_rise, ft_success, ft_time, ht_last_min, ft_last_min, date, ht_probability, ft_probability, ht_prediction, id) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'

class ResultRepository:
    __cursor :pyodbc.Cursor = None
    __cache = None
    
    def __init__(self, connection_string:str, loggerFactory):
        conn = pyodbc.connect(connection_string)
        self.__cursor = conn.cursor()
        self.__cache = {}
        self.__logger = loggerFactory.getLogger("Repository")
    
    def __MapToDto(self, result) -> ResultDto:
        dto = ResultDto(result.id, result.ht_time, result.ht_odd)
        dto.ht_prematch_odd = result.ht_prematch_odd
        dto.ht_prematch_goalline = result.ht_prematch_goalline
        dto.ht_rise = result.ht_rise
        dto.ht_success = result.ht_success
        dto.ft_odd = result.ft_odd
        dto.ft_prematch_odd = result.ft_prematch_odd
        dto.ft_prematch_goalline = result.ft_prematch_goalline
        dto.ft_rise = result.ft_rise
        dto.ft_success = result.ft_success
        dto.ft_time = result.ft_time
        dto.ht_last_min = result.ht_last_min
        dto.ft_last_min = result.ft_last_min
        dto.match_date = result.date
        dto.ht_prob = result.ht_probability
        dto.ft_prob = result.ft_probability
        dto.ht_pred = result.ht_prediction
        return dto
    
    def __MapFromDto(self, dto :ResultDto) -> tuple:
        return (dto.ht_time, 
                dto.ht_odd, 
                dto.ht_prematch_odd, 
                dto.ht_prematch_goalline, 
                dto.ht_rise, 
                dto.ht_success, 
                dto.ft_odd, 
                dto.ft_prematch_odd, 
                dto.ft_prematch_goalline, 
                dto.ft_rise, 
                dto.ft_success, 
                dto.ft_time, 
                dto.ht_last_min,
                dto.ft_last_min,
                dto.match_date,
                dto.ht_prob,
                dto.ft_prob,
                dto.ht_pred,
                dto.id)
    
       
    def GetResultById(self, match_id:str) -> ResultDto:
        self.__logger.debug(f"正從資料庫取得{match_id}賽事的資料")
        result = self.__cursor.execute(SELECT_QUERY + "WHERE id = ?", match_id).fetchone()
        if result is None:
            self.__logger.debug(f"資料庫不存在{match_id}的資料")
            return None
        
        return self.__MapToDto(result)
        
    
    def GetResults(self, read_from_cache :bool) -> List[ResultDto]:
        if read_from_cache and len(self.__cache) > 0:
            return list(self.__cache.values())
        
        self.__logger.debug(f"正從資料庫取得所有賽事的資料")
        results = self.__cursor.execute(SELECT_QUERY).fetchall()
        to_return = []
        for result in results:
            dto = self.__MapToDto(result)
            to_return.append(dto)
            self.__cache[dto.id] = dto
            
        return list(self.__cache.values())
            
    def Upsert(self, dto:ResultDto):
        is_new = self.GetResultById(dto.id) is None
        data = self.__MapFromDto(dto)
        if is_new:
            self.__logger.debug(f"正新增至資料庫{dto.id}賽事資料")
            self.__cursor.execute(INSERT_QUERY, data)
            self.__cursor.commit()
        else:
            self.__logger.debug(f"正更新資料庫{dto.id}賽事資料")
            self.__cursor.execute(UPDATE_QUERY, data)
            self.__cursor.commit()
                
if __name__ == "__main__":
    from Config import *
    from bs4 import BeautifulSoup
    import requests
    recorder = ResultRepository(CONNECTION_STRING)
    #ids = recorder.GetUnfinishedMatchIds()
    #print(ids)
    
    '''
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
    match_ids = cursor.execute("select id from [HKJC_Odds].[dbo].[live_match]").fetchall()
    for id_list in match_ids:
        id = id_list[0]
        odds = FindGoalLineOdds(f"/match/{id}/odds", 'fhl')
        one_five = odds['1.5']
        cursor.execute("update [HKJC_Odds].[dbo].[live_match] set ht_prematch_odd = ? where id = ?", (one_five, id))
        cursor.commit()
    
    #recorder = Recorder(connection_string)
    #print(recorder.GetSuccessRateByTime(13,16))
    '''