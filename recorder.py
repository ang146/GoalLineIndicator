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
    
    def WriteMatch(self, match_id:str, recorded_time:int, recorded_odd:float):
        is_new = self.CheckIsNew(match_id)
        data = [(match_id, recorded_time, recorded_odd)]
        if is_new:
            self.cursor.executemany("insert into [HKJC_Odds].[dbo].[half_time_live] (match_id, recorded_time, recorded_odd) values (?, ?, ?)", data)
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
    recorder = Recorder(connection_string)
    print(recorder.GetSuccessRateByTime(13,16))