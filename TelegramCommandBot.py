from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
from LoggerFactory import LoggerFactory
from Config import *
from DataAccess.ResultRepository import ResultRepository
from DataAccess.ResultDto import ResultDto
from datetime import datetime
import pytz
import matplotlib.pyplot as plt
import io
from PIL import Image
import tempfile
import numpy as np
from typing import List

SECONDS_IN_DAY = 86400
loggerFact = LoggerFactory("CommandBot_Logs")
repo = ResultRepository(CONNECTION_STRING, loggerFact)
    
async def DataByDayCommand(update : Update, context :ContextTypes.DEFAULT_TYPE):
    currentTime = datetime.now(tz=pytz.timezone('Asia/Hong_Kong'))
    requestDays, recentMatches, *_ = await GetMatchesWithinDate(update, context, currentTime)
    
    accurracyDict = {}
    for day in range(requestDays):
        correct = 0
        total = 0
        for recentMatch in recentMatches:
            matchDate = pytz.timezone('Asia/Hong_Kong').localize(recentMatch.match_date)
            matchDaysInSec = (currentTime - matchDate).total_seconds()
            if not (matchDaysInSec >= day * SECONDS_IN_DAY and matchDaysInSec < (day + 1) * SECONDS_IN_DAY):
                continue
            
            if not recentMatch.ht_prob is None and not recentMatch.ht_success is None:
                if (recentMatch.ht_prob > 50 and recentMatch.ht_success) or (recentMatch.ht_prob < 50 and not recentMatch.ht_success):
                    correct += 1
                if recentMatch.ht_prob != 50:
                    total += 1
            if not recentMatch.ft_prob is None and not recentMatch.ft_success is None:
                if (recentMatch.ft_prob > 50 and recentMatch.ft_success) or (recentMatch.ft_prob < 50 and not recentMatch.ft_success):
                    correct += 1
                if recentMatch.ft_prob != 50:
                    total += 1
        
        rate = 0
        if total != 0:
            rate = (correct / total) * 100
        
        accurracyDict[day] = (rate, total)

    days = list(accurracyDict.keys())
    rates = [value[0] for value in accurracyDict.values()]
    
    returnMessage = f"近{7 if requestDays >= 7 else requestDays}日數據庫分析趨勢\n" + "\n".join(f'{f"{day}日前" if day > 0 else "今日"}: {value[0]:.2f}% (共{value[1]}場次)' for day, value in reversed(accurracyDict.items()) if day <= 6)
    temp_filename = GenerateTrendGraph(days, rates, '日數(左為最近)')
    
    await update.message.reply_photo(open(temp_filename, 'rb'), caption=returnMessage)
    import os
    os.unlink(temp_filename)
    
async def PredictionByDays(update : Update, context :ContextTypes.DEFAULT_TYPE, predict :bool):
    currentTime = datetime.now(tz=pytz.timezone('Asia/Hong_Kong'))
    requestDays, matches, *_ = await GetMatchesWithinDate(update, context, currentTime)
    
    accurracyDict = {}
    
    for day in range(requestDays):
        
        correct = 0
        total = 0
        for match in matches:
            matchDate = pytz.timezone('Asia/Hong_Kong').localize(match.match_date)
            matchDaysInSec = (currentTime - matchDate).total_seconds()
            if not (matchDaysInSec >= day * SECONDS_IN_DAY and matchDaysInSec < (day + 1) * SECONDS_IN_DAY):
                continue
            
            if match.ht_pred is None:
                continue
            if match.ht_success is None:
                continue
        
            if predict:
                if match.ht_pred:
                    if match.ht_success >= 1:
                        correct += 1
                    total += 1
            else:
                if not match.ht_pred:
                    if not match.ht_success:
                        correct += 1
                    total += 1
            matches.remove(match)
                
        rate = 0
        if total != 0:
            rate = (correct / total) * 100
        
        accurracyDict[day] = (rate, total)
    
    days = list(accurracyDict.keys())
    rates = [value[0] for value in accurracyDict.values()]
    
    returnMessage = f"近{7 if requestDays >= 7 else requestDays}預測日趨勢\n" + "\n".join(f'{f"{day}日前" if day > 0 else "今日"}: {value[0]:.2f}% (共{value[1]}場次)' for day, value in reversed(accurracyDict.items()) if day <= 6)
    temp_filename = GenerateTrendGraph(days, rates, '日數(左為最近)')
    
    await update.message.reply_photo(open(temp_filename, 'rb'), caption=returnMessage)
    import os
    os.unlink(temp_filename)

def GetRoadGraph(matches :List[ResultDto], predict :bool) -> str:    
    listRoad = []
    turnedDict = {}
    
    def AddToRoad(success :bool):
        if len(listRoad) == 0:
            listRoad.append([success, None, None, None, None, None])
            return
        
        latestValidRoad = 0
        for i in range(len(listRoad)):
            if listRoad[i][0] is None:
                break
            latestValidRoad = i
        
        if listRoad[latestValidRoad][0] != success:
            latestValidRoad += 1
            
        if len(listRoad) <= latestValidRoad:
            listRoad.append([None for _ in range(6)])
            
        filled = False
        for i in range(6):
            if listRoad[latestValidRoad][i] is None:
                listRoad[latestValidRoad][i] = success
                filled = True
                break
            
        if not filled:
            toSetRow = 5
            for row, turningData in turnedDict.items():
                if row == latestValidRoad:
                    continue
                
                if row + turningData[1] >= latestValidRoad:
                    toSetRow = turningData[0] - 1
            if toSetRow < 0:
                toSetRow = 0
            if not latestValidRoad in turnedDict:
                turnedDict[latestValidRoad] = [toSetRow, 0]
                    
            extended = latestValidRoad + 1
            while not filled:
                if extended >= len(listRoad):
                    listRoad.append([None for _ in range(6)])
                    listRoad[extended][toSetRow] = success
                    turnedDict[latestValidRoad] = [toSetRow, turnedDict[latestValidRoad][1] + 1]
                    filled = True
                    break
                if not listRoad[extended][toSetRow] is None:
                    extended += 1
                    filled = False
                    pass
                else:
                    listRoad[extended][toSetRow] = success
                    turnedDict[latestValidRoad] = [toSetRow, turnedDict[latestValidRoad][1] + 1]
                    filled = True
                    break
        
    for match in matches:
        if predict:
            if match.ht_success >= 1:
                AddToRoad(True)
            else:
                AddToRoad(False)
        else:
            if match.ht_success == 0:
                AddToRoad(True)
            else:
                AddToRoad(False)
                
    listRoad = listRoad[-14:]
                
    result = ""
    for row in range(6):
        for column in range(len(listRoad)):
            if listRoad[column][row] == True:
                result += '✅'
            elif listRoad[column][row] == False:
                result += '❌'
            else:
                result += '⬛️'
        result += "\n"
    
    return result
       
async def PredictionRecent(update :Update, context :ContextTypes.DEFAULT_TYPE, predict :bool):
    matches :List[ResultDto] = sorted(repo.GetResults(False), key=lambda x: x.id)
    matches = [x for x in matches if x.id > 1060 and not x.ht_success is None and not x.ht_pred is None and x.ht_pred == predict]
    
    roadMsg = GetRoadGraph(predict)
    
    matches = reversed(matches[-10:])
    replyMsg = f"牙bot預測近10場{'有' if predict else '無'}為:\n(最近)"
    for match in matches:
        if match.ht_success >= 1:
            if predict:
                replyMsg += '✅'
            else:
                replyMsg += '❌'
        else:
            if predict:
                replyMsg += '❌'
            else:
                replyMsg += '✅'
                
    replyMsg += "\n\n 最近預測路紙:\n" + roadMsg
    await update.message.reply_text(replyMsg)
    

async def PredictTrueByDaysCommand(update, context):
    await PredictionByDays(update, context, True)

async def PredictFalseByDaysCommand(update, context):
    await PredictionByDays(update, context, False)
    
async def PredictTrueRecentCommand(update, context):
    await PredictionRecent(update, context, True)
    
async def PredictFalseRecentCommand(update, context):
    await PredictionRecent(update, context, False)
    
async def GetMatchesWithinDate(update : Update, context :ContextTypes.DEFAULT_TYPE, currentTime :datetime) -> tuple[int, List[ResultDto]]:
    results = repo.GetResults(True)
    requestDays = 10
    if context.args:
        daysString = context.args[0]
        if not daysString.isdigit():
            await update.message.reply_text(f"輸入數值{daysString}不明, 將以預設10日計算")
        else:
            daysInt = int(daysString)
            if daysInt <= 2:
                await update.message.reply_text(f"輸入數值{daysInt}小於2, 將以預設10日計算")
            elif daysInt > 30:
                await update.message.reply_text(f"輸入數值{daysInt}太大, 將以30日計算")
                requestDays = 30
            else:
                requestDays = daysInt
    
    return (requestDays, [x for x in results if x.match_date is not None and (currentTime - pytz.timezone('Asia/Hong_Kong').localize(x.match_date)).total_seconds() <= (SECONDS_IN_DAY * requestDays)])

def GenerateTrendGraph(days :List[int], rates :List[float], x_lbl :str) -> str:
    x = np.linspace(0, len(days) - 1, 100)
    
    from scipy.interpolate import CubicSpline
    cs = CubicSpline(range(len(days)), rates)
    
    plt.rcParams['font.family'] = ['MingLiU', 'Arial', 'sans-serif']
    
    plt.plot(range(len(days)), rates, 'o', label="實數")
    plt.plot(x, cs(x), label="曲線圖")

    plt.ylim(0, 100)
    plt.xlabel(x_lbl)
    plt.ylabel('命中 (%)')
    plt.title('命中趨勢')
    plt.grid(True)
    plt.legend()
    
    image_buffer = io.BytesIO()
    plt.savefig(image_buffer, format='png')
    image_buffer.seek(0)
    plt.close()
    with tempfile.NamedTemporaryFile(suffix='.png', delete=False) as temp_file:
        temp_file.write(image_buffer.getvalue())
        temp_filename = temp_file.name    
    
    return temp_filename
            
async def errors(update : Update, context :ContextTypes.DEFAULT_TYPE):
    print(f"Update {update} caused the error {context.error}")
        

if __name__ == "__main__":
    app = Application.builder().token(TOKEN).build()
    print("starting bot...")
    
    app.add_handler(CommandHandler('data_day', DataByDayCommand))
    app.add_handler(CommandHandler('yes_day', PredictTrueByDaysCommand))
    app.add_handler(CommandHandler('no_day', PredictFalseByDaysCommand))
    app.add_handler(CommandHandler('yes_recent', PredictTrueRecentCommand))
    app.add_handler(CommandHandler('no_recent', PredictFalseRecentCommand))
    app.add_error_handler(errors)
    
    print("polling...")
    app.run_polling(poll_interval=5)