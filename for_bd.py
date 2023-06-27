import pandas as pd
import requests
from io import StringIO
import sqlite3
import json 

# for connect to google drive
import json
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials

def get_data_into_drive(): 
    # Specify path to your file with credentials
    path_to_credential = './keys/conn_to_drive.json' 

    # Specify name of table in google sheets
    table_name = 'full'

    scope = ['https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/drive']

    credentials = ServiceAccountCredentials.from_json_keyfile_name(path_to_credential, scope)

    gs = gspread.authorize(credentials)
    work_sheet = gs.open(table_name)

    # Select 1st sheet
    sheet1 = work_sheet.sheet1

    # Get data in python lists format
    data = sheet1.get_all_values()

    # Get header from data
    headers = data.pop(0)

    # Create df
    df = pd.DataFrame(data, columns=headers)
    return df

def saving_to_db(df):
    con = sqlite3.connect('data_table.db') #создаем ключ подключения к БД чтоб БД было постоянной

    df.to_sql('data_table.db', con, index=False, if_exists='replace')

def main():
    df = get_data_into_drive()
    saving_to_db(df)

#удобно завернуть ФУНКЦИЮ pd.read_sql в функцию обертку 
#def select(sql):
#  return pd.read_sql(sql, con)
#и теперь можно просто писать select(sql)

#sql = '''select * from data_table '''

#df = select(sql)
