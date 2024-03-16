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
    
async def data_command(update : Update, context :ContextTypes.DEFAULT_TYPE):
    currentTime = datetime.now(tz=pytz.timezone('Asia/Hong_Kong'))
    requestDays, recentMatches, *_ = await get_matches_within_dates(update, context, currentTime)
    
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
    temp_filename = generate_trend_graph(days, rates)
    
    await update.message.reply_photo(open(temp_filename, 'rb'), caption=returnMessage)
    import os
    os.unlink(temp_filename)
    
async def predict_command(update : Update, context :ContextTypes.DEFAULT_TYPE, predict :bool):
    currentTime = datetime.now(tz=pytz.timezone('Asia/Hong_Kong'))
    requestDays, matches, *_ = await get_matches_within_dates(update, context, currentTime)
    
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
                
        rate = 0
        if total != 0:
            rate = (correct / total) * 100
        
        accurracyDict[day] = (rate, total)
    
    days = list(accurracyDict.keys())
    rates = [value[0] for value in accurracyDict.values()]
    
    returnMessage = f"近{7 if requestDays >= 7 else requestDays}預測日趨勢\n" + "\n".join(f'{f"{day}日前" if day > 0 else "今日"}: {value[0]:.2f}% (共{value[1]}場次)' for day, value in reversed(accurracyDict.items()) if day <= 6)
    temp_filename = generate_trend_graph(days, rates)
    
    await update.message.reply_photo(open(temp_filename, 'rb'), caption=returnMessage)
    import os
    os.unlink(temp_filename)

async def predict_true_command(update, context):
    await predict_command(update, context, True)

async def predict_false_command(update, context):
    await predict_command(update, context, False)
    
async def get_matches_within_dates(update : Update, context :ContextTypes.DEFAULT_TYPE, currentTime :datetime) -> tuple[int, List[ResultDto]]:
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

def generate_trend_graph(days :List[int], rates :List[float]) -> str:
    x = np.linspace(0, len(days) - 1, 100)
    
    from scipy.interpolate import CubicSpline
    cs = CubicSpline(range(len(days)), rates)
    
    plt.rcParams['font.family'] = ['MingLiU', 'Arial', 'sans-serif']
    
    plt.plot(range(len(days)), rates, 'o', label='Original data')
    plt.plot(x, cs(x), label='Smooth line')

    plt.ylim(0, 100)
    plt.xlabel('日數(左為最近)')
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

async def message_handler(update : Update, context :ContextTypes.DEFAULT_TYPE):
    message_text = update.message.text
    bot_username = context.bot.username

    if bot_username in message_text:
        user_text = message_text.replace(bot_username, '').strip()

        if user_text:
            await update.message.reply_text(f"{user_text[0]}你老母")
        else:
            await update.message.reply_text("咩")
        

if __name__ == "__main__":
    app = Application.builder().token(TOKEN).build()
    print("starting bot...")
    
    app.add_handler(CommandHandler('data', data_command))
    app.add_handler(CommandHandler('preyes', predict_true_command))
    app.add_handler(CommandHandler('preno', predict_false_command))
    app.add_handler(MessageHandler(filters.TEXT, message_handler))

    
    print("polling...")
    app.run_polling(poll_interval=5)