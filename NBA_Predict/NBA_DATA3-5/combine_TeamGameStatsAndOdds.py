import pandas as pd
import requests,bs4
import json
from datetime import datetime
from datetime import datetime,timezone,timedelta
import traceback, pymssql
import time, os
from selenium import webdriver
import selenium
from selenium.webdriver.firefox.options import Options
from time import sleep # this should go at the top of the file
import threading
from threading import Lock
s_print_lock = Lock()
def CombineGameFromYesterday():
    # 得到時間
    ETDateTime = datetime.now()-timedelta(hours=13)
    # 設定範圍
    ETDateTimeTop, ETDateTimeBottom = ETDateTime.replace(hour=0, minute=0, second=0, microsecond=0)-timedelta(days=3), ETDateTime.replace(hour=23, minute=59, second=59, microsecond=0)
    ET_month = ETDateTimeTop.month
    ET_year =  ETDateTimeTop.year-1 if ET_month < 10 else ETDateTimeTop.year

    nba_df = pd.read_csv(f"C:/python_projects_temp/NBA_Predict/NBA_DATA3-5/{ET_year}_TeamGameStats.csv")
    nba_odds_df = pd.read_csv(f"C:/python_projects_temp/NBA_Predict/NBA_DATA3-5/{ET_year}_TeamGameOdds.csv")
    nba_df['gmDateTime'] = nba_df['gmDate'] + ' ' + nba_df['gmTime']
    nba_df['gmDateTime'] = pd.to_datetime(nba_df['gmDateTime'])
    nba_odds_df['gmDateTime'] = nba_odds_df['gmDate'] + ' ' + nba_odds_df['gmTime']
    nba_odds_df['gmDateTime'] = pd.to_datetime(nba_odds_df['gmDateTime'])

    print(datetime.now().strftime('%Y/%m/%d %H:%M:%S'),'-' * 10,f"RUN {ET_year}-{ET_year+1} CombineGameFromYesterday (ET. {ETDateTimeTop.strftime('%Y/%m/%d %H:%M')}-{ETDateTimeBottom.strftime('%Y/%m/%d %H:%M')})")
    print(datetime.now().strftime('%Y/%m/%d %H:%M:%S'),'-' * 10,f'合併 {ETDateTimeTop}-{ETDateTimeBottom}) 賽事')
    nba_df = nba_df[(nba_df.gmDateTime >= ETDateTimeTop) & (nba_df.gmDateTime <= ETDateTimeBottom)]
    com_nba_df = nba_df.copy()
    del com_nba_df['gmDateTime']
    for idx, game in nba_df.iterrows():
        nba_df_MatchTime = datetime.strptime(game['gmDate'] + ' ' + game['gmTime'], "%Y/%m/%d %H:%M") + timedelta(hours=13)
        offset_sec = 120 * 60
        timestamp = time.mktime(nba_df_MatchTime.timetuple())
        top = datetime.fromtimestamp(timestamp + offset_sec)
        bottom = datetime.fromtimestamp(timestamp - offset_sec)

        nba_odds_slice_df = nba_odds_df[((game['teamAbbr']==nba_odds_df.homeAbbr) & (game['opptAbbr']==nba_odds_df.awayAbbr) |
                           (game['opptAbbr'] == nba_odds_df.homeAbbr) & (game['teamAbbr'] == nba_odds_df.awayAbbr)) &
                           (nba_odds_df['gmDateTime']>=bottom) & (nba_odds_df['gmDateTime']<top) ][:1]
        print(datetime.now().strftime('%Y/%m/%d %H:%M:%S'),'-' * 10,f'{ET_year} - {idx} {nba_df.loc[idx,:].to_dict()}')
        for _,odds_slice in nba_odds_slice_df.iterrows():
            com_nba_df.loc[idx,'home1x2FirstOptionRate'] = odds_slice.home1x2FirstOptionRate,
            com_nba_df.loc[idx,'away1x2FirstOptionRate'] = odds_slice.away1x2FirstOptionRate,
            com_nba_df.loc[idx,'home1x2LastOptionRate'] = odds_slice.home1x2LastOptionRate,
            com_nba_df.loc[idx,'away1x2LastOptionRate'] = odds_slice.away1x2LastOptionRate,
            com_nba_df.loc[idx,'home1x2FirstWinRate'] = odds_slice.home1x2FirstWinRate,
            com_nba_df.loc[idx,'away1x2FirstWinRate'] = odds_slice.away1x2FirstWinRate,
            com_nba_df.loc[idx,'home1x2LastWinRate'] = odds_slice.home1x2LastWinRate,
            com_nba_df.loc[idx,'away1x2LastWinRate'] = odds_slice.away1x2LastWinRate,
            com_nba_df.loc[idx,'home1x2FirstRTP'] = odds_slice.home1x2FirstRTP,
            com_nba_df.loc[idx,'away1x2LastRTP'] = odds_slice.away1x2LastRTP,
            com_nba_df.loc[idx,'home1x2KellyIndex'] = odds_slice.home1x2KellyIndex,
            com_nba_df.loc[idx,'away1x2KellyIndex'] = odds_slice.away1x2KellyIndex,

            com_nba_df.loc[idx,'OverFirstOptionRate'] = odds_slice.OverFirstOptionRate,
            com_nba_df.loc[idx,'UnderFirstOptionRate'] = odds_slice.UnderFirstOptionRate,
            com_nba_df.loc[idx,'OverUnderFirstSpecialBetValue'] = odds_slice.OverUnderFirstSpecialBetValue,
            com_nba_df.loc[idx,'OverLastOptionRate'] = odds_slice.OverLastOptionRate,
            com_nba_df.loc[idx,'UnderLastOptionRate'] = odds_slice.UnderLastOptionRate,
            com_nba_df.loc[idx,'OverUnderLastSpecialBetValue'] = odds_slice.OverUnderLastSpecialBetValue,
            com_nba_df.loc[idx,'SourceUrl'] = odds_slice.SourceUrl

            print(datetime.now().strftime('%Y/%m/%d %H:%M:%S'),'-' * 10,f'結合後 -- > {com_nba_df.loc[idx,:].to_dict()}')


    filepath = f'C:/python_projects_temp/NBA_Predict/NBA_DATA3-5/{ET_year}_TeamGameStatsAndOdds.csv'
    print(datetime.now().strftime('%Y/%m/%d %H:%M:%S'), '-' * 10, f'{ET_year}-{ET_year + 1} 賽事合併表寫入 --> {filepath} ')
    if os.path.isfile(filepath):
        pd.concat([pd.read_csv(filepath), pd.DataFrame(com_nba_df)], ignore_index=True).drop_duplicates(
            subset=['gmDate', 'gmTime', 'teamAbbr', 'opptAbbr'], keep='last').to_csv(filepath, header=True, index=False)
    else:
        pd.DataFrame(com_nba_df).to_csv(filepath, header=True, index=False)


def CombineGame(year):
    nba_df = pd.read_csv(f"{year}_TeamGameStats.csv")
    nba_odds_df = pd.read_csv(f"{year}_TeamGameOdds.csv")
    nba_odds_df['gmDateTime'] = nba_odds_df['gmDate'] + ' ' + nba_odds_df['gmTime']
    nba_odds_df['gmDateTime'] = pd.to_datetime(nba_odds_df['gmDateTime'] )
    com_nba_df = nba_df.copy()
    for idx, game in nba_df.iterrows():
        nba_df_MatchTime = datetime.strptime(game['gmDate'] + ' ' + game['gmTime'], "%Y/%m/%d %H:%M") + timedelta(hours=13)
        offset_sec = 120 * 60
        timestamp = time.mktime(nba_df_MatchTime.timetuple())
        top = datetime.fromtimestamp(timestamp + offset_sec)
        bottom = datetime.fromtimestamp(timestamp - offset_sec)

        nba_odds_slice_df = nba_odds_df[((game['teamAbbr']==nba_odds_df.homeAbbr) & (game['opptAbbr']==nba_odds_df.awayAbbr) |
                           (game['opptAbbr'] == nba_odds_df.homeAbbr) & (game['teamAbbr'] == nba_odds_df.awayAbbr)) &
                           (nba_odds_df['gmDateTime']>=bottom) & (nba_odds_df['gmDateTime']<top) ][:1]
        with s_print_lock:
            print(datetime.now().strftime('%Y/%m/%d %H:%M:%S'),'-' * 10,f'{year} - {idx} {nba_df.loc[idx,:].to_dict()}')
        for _,odds_slice in nba_odds_slice_df.iterrows():
            com_nba_df.loc[idx,'home1x2FirstOptionRate'] = odds_slice.home1x2FirstOptionRate,
            com_nba_df.loc[idx,'away1x2FirstOptionRate'] = odds_slice.away1x2FirstOptionRate,
            com_nba_df.loc[idx,'home1x2LastOptionRate'] = odds_slice.home1x2LastOptionRate,
            com_nba_df.loc[idx,'away1x2LastOptionRate'] = odds_slice.away1x2LastOptionRate,
            com_nba_df.loc[idx,'home1x2FirstWinRate'] = odds_slice.home1x2FirstWinRate,
            com_nba_df.loc[idx,'away1x2FirstWinRate'] = odds_slice.away1x2FirstWinRate,
            com_nba_df.loc[idx,'home1x2LastWinRate'] = odds_slice.home1x2LastWinRate,
            com_nba_df.loc[idx,'away1x2LastWinRate'] = odds_slice.away1x2LastWinRate,
            com_nba_df.loc[idx,'home1x2FirstRTP'] = odds_slice.home1x2FirstRTP,
            com_nba_df.loc[idx,'away1x2LastRTP'] = odds_slice.away1x2LastRTP,
            com_nba_df.loc[idx,'home1x2KellyIndex'] = odds_slice.home1x2KellyIndex,
            com_nba_df.loc[idx,'away1x2KellyIndex'] = odds_slice.away1x2KellyIndex,

            com_nba_df.loc[idx,'OverFirstOptionRate'] = odds_slice.OverFirstOptionRate,
            com_nba_df.loc[idx,'UnderFirstOptionRate'] = odds_slice.UnderFirstOptionRate,
            com_nba_df.loc[idx,'OverUnderFirstSpecialBetValue'] = odds_slice.OverUnderFirstSpecialBetValue,
            com_nba_df.loc[idx,'OverLastOptionRate'] = odds_slice.OverLastOptionRate,
            com_nba_df.loc[idx,'UnderLastOptionRate'] = odds_slice.UnderLastOptionRate,
            com_nba_df.loc[idx,'OverUnderLastSpecialBetValue'] = odds_slice.OverUnderLastSpecialBetValue,
            com_nba_df.loc[idx,'SourceUrl'] = odds_slice.SourceUrl
            with s_print_lock:
                print(datetime.now().strftime('%Y/%m/%d %H:%M:%S'),'-' * 10,f'結合後 -- > {com_nba_df.loc[idx,:].to_dict()}')
    com_nba_df.to_csv(f'{year}_TeamGameStatsAndOdds.csv',index=False)

def main():

    tsk = []
    start_year = 2014
    end_year = 2021

    for year in range(start_year,end_year+1):
        t = threading.Thread(target=CombineGame, args=(year,))
        t.start()
        tsk.append(t)

    for t in tsk:
        t.join()

if __name__ == '__main__':
    CombineGameFromYesterday()


