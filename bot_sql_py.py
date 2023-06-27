import pandas as pd
import requests
from io import StringIO
from datetime import timedelta
import datetime as dt
import telebot
from telebot import types
import sqlite3
import json
import for_bd


# open json file with (token & chat_id)
with open('./keys/cred.json', 'r') as f:
    cred = json.load(f)

#connection 

token = cred['token'] 
chat_id = cred['chat_id']   


bot = telebot.TeleBot(token)




#convenient to wrap the pd.read_sql  in a wrapper function
def select(con):
  return pd.read_sql('''select * from data_table''', con)

def get_df_metrics(): #function for loading, processing data and calculating metrics
    con = sqlite3.connect('data_table.db')
    df = select(con)

    df['date'] = df['date'].astype("datetime64[ns]")

    metrics = df.groupby('date', as_index = False)\
                .agg({'id': 'count', 'paid': 'sum','rev': 'sum'})\
                .rename(columns = {'id': 'users', 'paid': 'pay_users'})\
                .assign(not_pay_users = lambda x: x.users - x.pay_users, \
                        arpu = lambda x:round((x.rev / x.users),2), \
                        arppu = lambda x: round((x.rev / x.pay_users),2),\
                        cr = lambda x: round((x.pay_users / x.users) * 100,2))
    df_metrics = metrics[['arpu', 'arppu', 'cr', 'date']]
    return df_metrics # the table will be needed to calculate the change in metrics for the last and previous day


@bot.message_handler(commands=['start']) 
def hello(message):
    keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=False) # view button
    keyboard.add(types.KeyboardButton('Send report'), types.KeyboardButton('dynamic')) #name button
    for_bd.main()
    bot.send_message(message.chat.id, f'hello  {message.from_user.first_name}', reply_markup=keyboard)

@bot.message_handler(content_types=['text'])
def main_menu(message):
    if message.text == 'Send report':
        df_metrics = get_df_metrics()
        df_metrics.to_csv('report.csv', index=False)
        bot.send_document(chat_id, open('report.csv'))

    elif message.text == 'dynamic':
        df_metrics = get_df_metrics()
        df_metrics = df_metrics.set_index('date')

        diff_metrics = pd.DataFrame(round((-df_metrics.iloc[-2] + df_metrics.iloc[-1])/df_metrics.iloc[-2]*100, 2)).T # calculate the change in metrics for the last and previous day
        diff_metrics.to_csv('dynamic.csv', index=False)
        bot.send_document(chat_id, open('dynamic.csv'))

bot.polling(none_stop=True)