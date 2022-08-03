import pandas as pd
import requests,bs4
import json
from datetime import datetime
from datetime import datetime,timezone,timedelta
import traceback, pymssql
import time
from selenium import webdriver
import selenium
from selenium.webdriver.firefox.options import Options
from time import sleep # this should go at the top of the file
import threading
from threading import Lock
import os

MatchEntrys = {}
GameStats = {}
s_print_lock = Lock()

def get_TeamGameStatsFromYesterday(ETDateTimeTop,ETDateTimeBottom,ET_year,ET_month):
    print(datetime.now().strftime('%Y/%m/%d %H:%M:%S'), '-' * 10, f"RUN get_TeamGameStatsFromDateTimeRange(ET. {ETDateTimeTop.strftime('%Y/%m/%d %H:%M')}-{ETDateTimeBottom.strftime('%Y/%m/%d %H:%M')})")
    if MatchEntrys[ET_year] == []:
            print(datetime.now().strftime('%Y/%m/%d %H:%M:%S'), '-' * 10,'MatchEntrys尚未爬取---')
    useragent = "Mozilla/5.0 (Linux; Android 8.0.0; Pixel 2 XL Build/OPD1.170816.004) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.87 Mobile Safari/537.36"
    profile = webdriver.FirefoxProfile()
    profile.set_preference("general.useragent.override", useragent)
    options = webdriver.FirefoxOptions()
    options.set_preference("dom.webnotifications.serviceworker.enabled", False)
    options.set_preference("dom.webnotifications.enabled", False)
    options.add_argument('--headless')
    browser = webdriver.Firefox(firefox_profile=profile, options=options)
    for idx, MatchEntry in enumerate(MatchEntrys[ET_year]):
        browser.get(MatchEntry['box_score_url'])
        sleep(1)
        objSoup = bs4.BeautifulSoup(browser.find_element_by_xpath('//*').get_attribute('outerHTML'), 'lxml')
        try:
            tfoots = objSoup.select(" table.sortable.stats_table.now_sortable tfoot ")
            OpptGameStats = dict(gmDate=process_DataTime(MatchEntry['date'], MatchEntry['time']).strftime('%Y/%m/%d'),
                                 gmTime=process_DataTime(MatchEntry['date'], MatchEntry['time']).strftime('%H:%M'),
                                 seasType='Regular',
                                 offLNm1='', offFNm1='',
                                 offLNm2='', offFNm2='',
                                 OffLNm3='', offFNm3='',
                                 teamAbbr=MatchEntry['away'],
                                 teamConf='', teamLoc='Away', teamRslt='Win' if int(MatchEntry['home_score']) < int(MatchEntry['away_score']) else 'Loss',
                                 teamMin=tfoots[0].select(" tr td[data-stat='mp'] ")[0].getText(),
                                 teamDayOff='',
                                 teamPTS=MatchEntry['away_score'],
                                 teamAST=tfoots[0].select(" tr td[data-stat='ast'] ")[0].getText(),
                                 teamTO=tfoots[0].select(" tr td[data-stat='tov'] ")[0].getText(),
                                 teamSTL=tfoots[0].select(" tr td[data-stat='stl'] ")[0].getText(),
                                 teamBLK=tfoots[0].select(" tr td[data-stat='blk'] ")[0].getText(),
                                 teamFGA=tfoots[0].select(" tr td[data-stat='fga'] ")[0].getText(),
                                 teamFGM=tfoots[0].select(" tr td[data-stat='fg'] ")[0].getText(),
                                 teamFG_Percentage=tfoots[0].select(" tr td[data-stat='fg_pct'] ")[0].getText(),
                                 team2PA='',
                                 team2PM='',
                                 team2P_Percentage='',
                                 team3PA=tfoots[0].select(" tr td[data-stat='fg3a'] ")[0].getText(),
                                 team3PM=tfoots[0].select(" tr td[data-stat='fg3'] ")[0].getText(),
                                 team3P_Percentage=tfoots[0].select(" tr td[data-stat='fg3_pct'] ")[0].getText(),
                                 teamFTA=tfoots[0].select(" tr td[data-stat='fta'] ")[0].getText(),
                                 teamFTM=tfoots[0].select(" tr td[data-stat='ft'] ")[0].getText(),
                                 teamFT_Percentage=tfoots[0].select(" tr td[data-stat='ft_pct'] ")[0].getText(),
                                 teamORB=tfoots[0].select(" tr td[data-stat='orb'] ")[0].getText(),
                                 teamDRB=tfoots[0].select(" tr td[data-stat='drb'] ")[0].getText(),
                                 teamTRB=tfoots[0].select(" tr td[data-stat='trb'] ")[0].getText(),
                                 teamPTS1='',
                                 teamPTS2='',
                                 teamPTS3='',
                                 teamPTS4='',
                                 teamPTS5='',
                                 teamPTS6='',
                                 teamPTS7='',
                                 teamPTS8='',
                                 teamTREB_Percentage=tfoots[1].select(" tr td[data-stat='trb_pct'] ")[0].getText(),
                                 teamASST_Percentage=tfoots[1].select(" tr td[data-stat='ast_pct'] ")[0].getText(),
                                 teamTS_Percentage=tfoots[1].select(" tr td[data-stat='ts_pct'] ")[0].getText(),
                                 teamEFG_Percentage=tfoots[1].select(" tr td[data-stat='efg_pct'] ")[0].getText(),
                                 teamOREB_Percentage=tfoots[1].select(" tr td[data-stat='orb_pct'] ")[0].getText(),
                                 teamDREB_Percentage=tfoots[1].select(" tr td[data-stat='drb_pct'] ")[0].getText(),
                                 teamTO_Percentage=tfoots[1].select(" tr td[data-stat='tov_pct'] ")[0].getText(),
                                 teamSTL_Percentage=tfoots[1].select(" tr td[data-stat='stl_pct'] ")[0].getText(),
                                 teamBLK_Percentage=tfoots[1].select(" tr td[data-stat='blk_pct'] ")[0].getText(),
                                 teamBLKR='',
                                 teamPPS='',
                                 teamFIC='',
                                 teamFIC40='',
                                 teamOrtg=tfoots[1].select(" tr td[data-stat='off_rtg'] ")[0].getText(),
                                 teamDrtg=tfoots[1].select(" tr td[data-stat='def_rtg'] ")[0].getText(),
                                 teamEDiff='',
                                 teamPlay_Percentage='',
                                 teamAR='',
                                 teamASTDividedByTO=round(int(tfoots[0].select(" tr td[data-stat='ast'] ")[0].getText()) / int(tfoots[0].select(" tr td[data-stat='tov'] ")[0].getText()), 2),
                                 teamSTLDividedByTO=round(int(tfoots[0].select(" tr td[data-stat='stl'] ")[0].getText()) / int(tfoots[0].select(" tr td[data-stat='tov'] ")[0].getText()) * 100, 2),
                                 teamFouls=tfoots[0].select(" tr td[data-stat='pf'] ")[0].getText(),
                                 opptAbbr=MatchEntry['home'],
                                 opptConf='', opptLoc='Home', opptRslt='Win' if int(MatchEntry['home_score']) > int(MatchEntry['away_score']) else 'Loss',
                                 opptMin=tfoots[2].select(" tr td[data-stat='mp'] ")[0].getText(),
                                 opptDayOff='',
                                 opptPTS=MatchEntry['home_score'],
                                 opptAST=tfoots[2].select(" tr td[data-stat='ast'] ")[0].getText(),
                                 opptTO=tfoots[2].select(" tr td[data-stat='tov'] ")[0].getText(),
                                 opptSTL=tfoots[2].select(" tr td[data-stat='stl'] ")[0].getText(),
                                 opptBLK=tfoots[2].select(" tr td[data-stat='blk'] ")[0].getText(),
                                 opptFGA=tfoots[2].select(" tr td[data-stat='fga'] ")[0].getText(),
                                 opptFGM=tfoots[2].select(" tr td[data-stat='fg'] ")[0].getText(),
                                 opptFG_Percentage=tfoots[2].select(" tr td[data-stat='fg_pct'] ")[0].getText(),
                                 oppt2PA='',
                                 oppt2PM='',
                                 oppt2P_Percentage='',
                                 oppt3PA=tfoots[2].select(" tr td[data-stat='fg3a'] ")[0].getText(),
                                 oppt3PM=tfoots[2].select(" tr td[data-stat='fg3'] ")[0].getText(),
                                 oppt3P_Percentage=tfoots[2].select(" tr td[data-stat='fg3_pct'] ")[0].getText(),
                                 opptFTA=tfoots[2].select(" tr td[data-stat='fta'] ")[0].getText(),
                                 opptFTM=tfoots[2].select(" tr td[data-stat='ft'] ")[0].getText(),
                                 opptFT_Percentage=tfoots[2].select(" tr td[data-stat='ft_pct'] ")[0].getText(),
                                 opptORB=tfoots[2].select(" tr td[data-stat='orb'] ")[0].getText(),
                                 opptDRB=tfoots[2].select(" tr td[data-stat='drb'] ")[0].getText(),
                                 opptTRB=tfoots[2].select(" tr td[data-stat='trb'] ")[0].getText(),
                                 opptPTS1='',
                                 opptPTS2='',
                                 opptPTS3='',
                                 opptPTS4='',
                                 opptPTS5='',
                                 opptPTS6='',
                                 opptPTS7='',
                                 opptPTS8='',
                                 opptTREB_Percentage=tfoots[3].select(" tr td[data-stat='trb_pct'] ")[0].getText(),
                                 opptASST_Percentage=tfoots[3].select(" tr td[data-stat='ast_pct'] ")[0].getText(),
                                 opptTS_Percentage=tfoots[3].select(" tr td[data-stat='ts_pct'] ")[0].getText(),
                                 opptEFG_Percentage=tfoots[3].select(" tr td[data-stat='efg_pct'] ")[0].getText(),
                                 opptOREB_Percentage=tfoots[3].select(" tr td[data-stat='orb_pct'] ")[0].getText(),
                                 opptDREB_Percentage=tfoots[3].select(" tr td[data-stat='drb_pct'] ")[0].getText(),
                                 opptTO_Percentage=tfoots[3].select(" tr td[data-stat='tov_pct'] ")[0].getText(),
                                 opptSTL_Percentage=tfoots[3].select(" tr td[data-stat='stl_pct'] ")[0].getText(),
                                 opptBLK_Percentage=tfoots[3].select(" tr td[data-stat='blk_pct'] ")[0].getText(),
                                 opptBLKR='',
                                 opptPPS='',
                                 opptFIC='',
                                 opptFIC40='',
                                 opptOrtg=tfoots[3].select(" tr td[data-stat='off_rtg'] ")[0].getText(),
                                 opptDrtg=tfoots[3].select(" tr td[data-stat='def_rtg'] ")[0].getText(),
                                 opptEDiff='',
                                 opptPlay_Percentage='',
                                 opptAR='',
                                 opptASTDividedByTO=round(int(tfoots[2].select(" tr td[data-stat='ast'] ")[0].getText()) / int(tfoots[2].select(" tr td[data-stat='tov'] ")[0].getText()), 2),
                                 opptSTLDividedByTO=round(int(tfoots[2].select(" tr td[data-stat='stl'] ")[0].getText()) / int(tfoots[2].select(" tr td[data-stat='tov'] ")[0].getText()) * 100, 2),
                                 opptFouls=tfoots[2].select(" tr td[data-stat='pf'] ")[0].getText())

            TeamGameStats = dict(gmDate=process_DataTime(MatchEntry['date'], MatchEntry['time']).strftime('%Y/%m/%d'),
                                 gmTime=process_DataTime(MatchEntry['date'], MatchEntry['time']).strftime('%H:%M'),
                                 seasType='Regular',
                                 offLNm1='', offFNm1='',
                                 offLNm2='', offFNm2='',
                                 OffLNm3='', offFNm3='',
                                 teamAbbr=MatchEntry['home'],
                                 teamConf='', teamLoc='Home',
                                 teamRslt='Win' if int(MatchEntry['home_score']) > int(MatchEntry['away_score']) else 'Loss',
                                 teamMin=tfoots[2].select(" tr td[data-stat='mp'] ")[0].getText(),
                                 teamDayOff='',
                                 teamPTS=MatchEntry['home_score'],
                                 teamAST=tfoots[2].select(" tr td[data-stat='ast'] ")[0].getText(),
                                 teamTO=tfoots[2].select(" tr td[data-stat='tov'] ")[0].getText(),
                                 teamSTL=tfoots[2].select(" tr td[data-stat='stl'] ")[0].getText(),
                                 teamBLK=tfoots[2].select(" tr td[data-stat='blk'] ")[0].getText(),
                                 teamFGA=tfoots[2].select(" tr td[data-stat='fga'] ")[0].getText(),
                                 teamFGM=tfoots[2].select(" tr td[data-stat='fg'] ")[0].getText(),
                                 teamFG_Percentage=tfoots[2].select(" tr td[data-stat='fg_pct'] ")[0].getText(),
                                 team2PA='',
                                 team2PM='',
                                 team2P_Percentage='',
                                 team3PA=tfoots[2].select(" tr td[data-stat='fg3a'] ")[0].getText(),
                                 team3PM=tfoots[2].select(" tr td[data-stat='fg3'] ")[0].getText(),
                                 team3P_Percentage=tfoots[2].select(" tr td[data-stat='fg3_pct'] ")[0].getText(),
                                 teamFTA=tfoots[2].select(" tr td[data-stat='fta'] ")[0].getText(),
                                 teamFTM=tfoots[2].select(" tr td[data-stat='ft'] ")[0].getText(),
                                 teamFT_Percentage=tfoots[2].select(" tr td[data-stat='ft_pct'] ")[0].getText(),
                                 teamORB=tfoots[2].select(" tr td[data-stat='orb'] ")[0].getText(),
                                 teamDRB=tfoots[2].select(" tr td[data-stat='drb'] ")[0].getText(),
                                 teamTRB=tfoots[2].select(" tr td[data-stat='trb'] ")[0].getText(),
                                 teamPTS1='',
                                 teamPTS2='',
                                 teamPTS3='',
                                 teamPTS4='',
                                 teamPTS5='',
                                 teamPTS6='',
                                 teamPTS7='',
                                 teamPTS8='',
                                 teamTREB_Percentage=tfoots[3].select(" tr td[data-stat='trb_pct'] ")[0].getText(),
                                 teamASST_Percentage=tfoots[3].select(" tr td[data-stat='ast_pct'] ")[0].getText(),
                                 teamTS_Percentage=tfoots[3].select(" tr td[data-stat='ts_pct'] ")[0].getText(),
                                 teamEFG_Percentage=tfoots[3].select(" tr td[data-stat='efg_pct'] ")[0].getText(),
                                 teamOREB_Percentage=tfoots[3].select(" tr td[data-stat='orb_pct'] ")[0].getText(),
                                 teamDREB_Percentage=tfoots[3].select(" tr td[data-stat='drb_pct'] ")[0].getText(),
                                 teamTO_Percentage=tfoots[3].select(" tr td[data-stat='tov_pct'] ")[0].getText(),
                                 teamSTL_Percentage=tfoots[3].select(" tr td[data-stat='stl_pct'] ")[0].getText(),
                                 teamBLK_Percentage=tfoots[3].select(" tr td[data-stat='blk_pct'] ")[0].getText(),
                                 teamBLKR='',
                                 teamPPS='',
                                 teamFIC='',
                                 teamFIC40='',
                                 teamOrtg=tfoots[3].select(" tr td[data-stat='off_rtg'] ")[0].getText(),
                                 teamDrtg=tfoots[3].select(" tr td[data-stat='def_rtg'] ")[0].getText(),
                                 teamEDiff='',
                                 teamPlay_Percentage='',
                                 teamAR='',
                                 teamASTDividedByTO=round(
                                     int(tfoots[2].select(" tr td[data-stat='ast'] ")[0].getText()) / int(
                                         tfoots[2].select(" tr td[data-stat='tov'] ")[0].getText()), 2),
                                 teamSTLDividedByTO=round(
                                     int(tfoots[2].select(" tr td[data-stat='stl'] ")[0].getText()) / int(
                                         tfoots[2].select(" tr td[data-stat='tov'] ")[0].getText()) * 100, 2),
                                 teamFouls=tfoots[2].select(" tr td[data-stat='pf'] ")[0].getText(),

                                 opptAbbr=MatchEntry['away'],
                                 opptConf='', opptLoc='Away',
                                 opptRslt='Win' if int(MatchEntry['home_score']) < int(
                                     MatchEntry['away_score']) else 'Loss',
                                 opptMin=tfoots[0].select(" tr td[data-stat='mp'] ")[0].getText(),
                                 opptDayOff='',
                                 opptPTS=MatchEntry['away_score'],
                                 opptAST=tfoots[0].select(" tr td[data-stat='ast'] ")[0].getText(),
                                 opptTO=tfoots[0].select(" tr td[data-stat='tov'] ")[0].getText(),
                                 opptSTL=tfoots[0].select(" tr td[data-stat='stl'] ")[0].getText(),
                                 opptBLK=tfoots[0].select(" tr td[data-stat='blk'] ")[0].getText(),
                                 opptFGA=tfoots[0].select(" tr td[data-stat='fga'] ")[0].getText(),
                                 opptFGM=tfoots[0].select(" tr td[data-stat='fg'] ")[0].getText(),
                                 opptFG_Percentage=tfoots[0].select(" tr td[data-stat='fg_pct'] ")[0].getText(),
                                 oppt2PA='',
                                 oppt2PM='',
                                 oppt2P_Percentage='',
                                 oppt3PA=tfoots[0].select(" tr td[data-stat='fg3a'] ")[0].getText(),
                                 oppt3PM=tfoots[0].select(" tr td[data-stat='fg3'] ")[0].getText(),
                                 oppt3P_Percentage=tfoots[0].select(" tr td[data-stat='fg3_pct'] ")[0].getText(),
                                 opptFTA=tfoots[0].select(" tr td[data-stat='fta'] ")[0].getText(),
                                 opptFTM=tfoots[0].select(" tr td[data-stat='ft'] ")[0].getText(),
                                 opptFT_Percentage=tfoots[0].select(" tr td[data-stat='ft_pct'] ")[0].getText(),
                                 opptORB=tfoots[0].select(" tr td[data-stat='orb'] ")[0].getText(),
                                 opptDRB=tfoots[0].select(" tr td[data-stat='drb'] ")[0].getText(),
                                 opptTRB=tfoots[0].select(" tr td[data-stat='trb'] ")[0].getText(),
                                 opptPTS1='',
                                 opptPTS2='',
                                 opptPTS3='',
                                 opptPTS4='',
                                 opptPTS5='',
                                 opptPTS6='',
                                 opptPTS7='',
                                 opptPTS8='',
                                 opptTREB_Percentage=tfoots[1].select(" tr td[data-stat='trb_pct'] ")[0].getText(),
                                 opptASST_Percentage=tfoots[1].select(" tr td[data-stat='ast_pct'] ")[0].getText(),
                                 opptTS_Percentage=tfoots[1].select(" tr td[data-stat='ts_pct'] ")[0].getText(),
                                 opptEFG_Percentage=tfoots[1].select(" tr td[data-stat='efg_pct'] ")[0].getText(),
                                 opptOREB_Percentage=tfoots[1].select(" tr td[data-stat='orb_pct'] ")[0].getText(),
                                 opptDREB_Percentage=tfoots[1].select(" tr td[data-stat='drb_pct'] ")[0].getText(),
                                 opptTO_Percentage=tfoots[1].select(" tr td[data-stat='tov_pct'] ")[0].getText(),
                                 opptSTL_Percentage=tfoots[1].select(" tr td[data-stat='stl_pct'] ")[0].getText(),
                                 opptBLK_Percentage=tfoots[1].select(" tr td[data-stat='blk_pct'] ")[0].getText(),
                                 opptBLKR='',
                                 opptPPS='',
                                 opptFIC='',
                                 opptFIC40='',
                                 opptOrtg=tfoots[1].select(" tr td[data-stat='off_rtg'] ")[0].getText(),
                                 opptDrtg=tfoots[1].select(" tr td[data-stat='def_rtg'] ")[0].getText(),
                                 opptEDiff='',
                                 opptPlay_Percentage='',
                                 opptAR='',
                                 opptASTDividedByTO=round(
                                     int(tfoots[0].select(" tr td[data-stat='ast'] ")[0].getText()) / int(
                                         tfoots[0].select(" tr td[data-stat='tov'] ")[0].getText()), 2),
                                 opptSTLDividedByTO=round(
                                     int(tfoots[0].select(" tr td[data-stat='stl'] ")[0].getText()) / int(
                                         tfoots[0].select(" tr td[data-stat='tov'] ")[0].getText()) * 100, 2),
                                 opptFouls=tfoots[0].select(" tr td[data-stat='pf'] ")[0].getText())
            print(datetime.now().strftime('%Y/%m/%d %H:%M:%S'), '-' * 10,'爬取 ','OpptGameStats --> ', OpptGameStats, '\n', 'TeamGameStats --> ', TeamGameStats)
            GameStats[ET_year].append(OpptGameStats)
            GameStats[ET_year].append(TeamGameStats)

        except selenium.common.exceptions.NoSuchElementException:
            print('找不到值.')
        except:
            traceback.print_exc()

        if idx + 1 == len(MatchEntrys[ET_year]):
            filepath = f'C:/python_projects_temp/NBA_Predict/NBA_DATA3-5/{ET_year}_TeamGameStats.csv'
            print(datetime.now().strftime('%Y/%m/%d %H:%M:%S'), '-' * 10, f'{ET_year}-{ET_year + 1} 賽事賠率寫入 --> {filepath} ')
            if os.path.isfile(filepath):
                pd.concat([pd.read_csv(filepath), pd.DataFrame(GameStats[ET_year])], ignore_index=True).drop_duplicates(
                    subset=['gmDate', 'gmTime', 'teamAbbr', 'opptAbbr'], keep='last').to_csv(filepath, header=True,
                                                                                             index=False)
            else:
                pd.DataFrame(GameStats[ET_year]).to_csv(filepath, header=True, index=False)

    browser.close()
    browser.quit()

def MonthNumberToText(num):
    NumberToText = {10:'october', 11:'november', 12:'december', 1:'january', 2:'february', 3:'march', 4:'april', 5:'may',6:'june', 7:'july'}
    return NumberToText[num]

def get_MatchEntrysFromDateYesterday():
    # 得到昨日美東時間
    ETDateTime = datetime.now()-timedelta(hours=13)-timedelta(days=1)
    # 設定範圍
    ETDateTimeTop, ETDateTimeBottom = ETDateTime.replace(hour=0, minute=0, second=0, microsecond=0),ETDateTime.replace(hour=23, minute=59, second=59, microsecond=0)

    ET_month = ETDateTimeTop.month
    ET_year =  ETDateTimeTop.year-1 if ET_month < 10 else ETDateTimeTop.year
    MatchEntrys[ET_year] = []
    GameStats[ET_year] = []

    useragent = "Mozilla/5.0 (Linux; Android 8.0.0; Pixel 2 XL Build/OPD1.170816.004) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.87 Mobile Safari/537.36"
    profile = webdriver.FirefoxProfile()
    options = webdriver.FirefoxOptions()
    profile.set_preference("general.useragent.override", useragent)
    options.set_preference("dom.webnotifications.serviceworker.enabled", False)
    options.set_preference("dom.webnotifications.enabled", False)
    options.add_argument('--headless')
    browser = webdriver.Firefox(firefox_profile=profile, options=options)
    try:
        url = f'https://www.basketball-reference.com/leagues/NBA_{ET_year+1}_games-{MonthNumberToText(ET_month)}.html'
        print(datetime.now().strftime('%Y/%m/%d %H:%M:%S'),'-' * 10,f"RUN get_MatchEntrysFromDateTimeRange(ET. {ETDateTimeTop.strftime('%Y/%m/%d %H:%M')}-{ETDateTimeBottom.strftime('%Y/%m/%d %H:%M')})({url})")
        browser.get(url)
        sleep(5)
        for tr in browser.find_elements_by_css_selector("#schedule tbody tr"):
            try:
                MatchEntry = dict(date=tr.find_element_by_css_selector('th[data-stat="date_game"]').text,
                                  time=tr.find_element_by_css_selector('td[data-stat="game_start_time"]').text,
                                  away=tr.find_element_by_css_selector('td[data-stat="visitor_team_name"]').text,
                                  away_score=tr.find_element_by_css_selector('td[data-stat="visitor_pts"]').text,
                                  home=tr.find_element_by_css_selector('td[data-stat="home_team_name"]').text,
                                  home_score=tr.find_element_by_css_selector('td[data-stat="home_pts"]').text,
                                  box_score_url=tr.find_element_by_css_selector(
                                      'td[data-stat="box_score_text"] a').get_attribute('href'))

                if process_DataTime(MatchEntry['date'], MatchEntry['time']) >= ETDateTimeTop and process_DataTime(MatchEntry['date'], MatchEntry['time']) <= ETDateTimeBottom:
                    print(datetime.now().strftime('%Y/%m/%d %H:%M:%S'), '-' * 10, MatchEntry)
                    MatchEntrys[ET_year].append(MatchEntry)
            except selenium.common.exceptions.NoSuchElementException:
                print('error field.')
            except:
                traceback.print_exc()

        get_TeamGameStatsFromYesterday(ETDateTimeTop,ETDateTimeBottom,ET_year,ET_month)
    except:
        traceback.print_exc()
    browser.close()
    browser.quit()

def process_DataTime(Date, Start):
    Date = Date.replace(',','')
    return datetime.strptime(Date + ' ' + Start[:-1] + 'PM', "%a %b %d %Y %I:%M%p")

def get_TeamGameStats(year):
    print('*' * 70)
    print('*' * 30, year, 'RUN get_TeamGameStats', '*' * 30)
    print('*' * 70)
    if MatchEntrys == {}:
        with s_print_lock:
            print('MatchEntrys尚未爬取---')

    useragent = "Mozilla/5.0 (Linux; Android 8.0.0; Pixel 2 XL Build/OPD1.170816.004) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.87 Mobile Safari/537.36"
    profile = webdriver.FirefoxProfile()
    profile.set_preference("general.useragent.override", useragent)
    options = webdriver.FirefoxOptions()
    options.set_preference("dom.webnotifications.serviceworker.enabled", False)
    options.set_preference("dom.webnotifications.enabled", False)
    options.add_argument('--headless')
    browser = webdriver.Firefox(firefox_profile=profile, options=options)

    for idx,MatchEntry in enumerate(MatchEntrys[year]):
        with s_print_lock:
            print('-' * 30)
            print(year,idx+1, MatchEntry)
        browser.get(MatchEntry['box_score_url'])
        sleep(1)
        objSoup = bs4.BeautifulSoup(browser.find_element_by_xpath('//*').get_attribute('outerHTML'), 'lxml')
        try:
            tfoots = objSoup.select(" table.sortable.stats_table.now_sortable tfoot ")

            OpptGameStats = dict(gmDate = process_DataTime(MatchEntry['date'], MatchEntry['time']).strftime('%Y/%m/%d'),
                                 gmTime = process_DataTime(MatchEntry['date'], MatchEntry['time']).strftime('%H:%M'),
                                 seasType = 'Regular',
                                 offLNm1='',offFNm1='',
                                 offLNm2='',offFNm2='',
                                 OffLNm3='',offFNm3='',
                                 teamAbbr=MatchEntry['away'],
                                 teamConf='',teamLoc='Away',teamRslt='Win' if int(MatchEntry['home_score']) < int(MatchEntry['away_score']) else 'Loss',
                                 teamMin=tfoots[0].select(" tr td[data-stat='mp'] ")[0].getText(),
                                 teamDayOff='',
                                 teamPTS=MatchEntry['away_score'],
                                 teamAST=tfoots[0].select(" tr td[data-stat='ast'] ")[0].getText(),
                                 teamTO=tfoots[0].select(" tr td[data-stat='tov'] ")[0].getText(),
                                 teamSTL=tfoots[0].select(" tr td[data-stat='stl'] ")[0].getText(),
                                 teamBLK=tfoots[0].select(" tr td[data-stat='blk'] ")[0].getText(),
                                 teamFGA=tfoots[0].select(" tr td[data-stat='fga'] ")[0].getText(),
                                 teamFGM=tfoots[0].select(" tr td[data-stat='fg'] ")[0].getText(),
                                 teamFG_Percentage=tfoots[0].select(" tr td[data-stat='fg_pct'] ")[0].getText(),
                                 team2PA='',
                                 team2PM='',
                                 team2P_Percentage='',
                                 team3PA=tfoots[0].select(" tr td[data-stat='fg3a'] ")[0].getText(),
                                 team3PM=tfoots[0].select(" tr td[data-stat='fg3'] ")[0].getText(),
                                 team3P_Percentage=tfoots[0].select(" tr td[data-stat='fg3_pct'] ")[0].getText(),
                                 teamFTA=tfoots[0].select(" tr td[data-stat='fta'] ")[0].getText(),
                                 teamFTM=tfoots[0].select(" tr td[data-stat='ft'] ")[0].getText(),
                                 teamFT_Percentage=tfoots[0].select(" tr td[data-stat='ft_pct'] ")[0].getText(),
                                 teamORB=tfoots[0].select(" tr td[data-stat='orb'] ")[0].getText(),
                                 teamDRB=tfoots[0].select(" tr td[data-stat='drb'] ")[0].getText(),
                                 teamTRB=tfoots[0].select(" tr td[data-stat='trb'] ")[0].getText(),
                                 teamPTS1='',
                                 teamPTS2='',
                                 teamPTS3='',
                                 teamPTS4='',
                                 teamPTS5='',
                                 teamPTS6='',
                                 teamPTS7='',
                                 teamPTS8='',
                                 teamTREB_Percentage=tfoots[1].select(" tr td[data-stat='trb_pct'] ")[0].getText(),
                                 teamASST_Percentage=tfoots[1].select(" tr td[data-stat='ast_pct'] ")[0].getText(),
                                 teamTS_Percentage=tfoots[1].select(" tr td[data-stat='ts_pct'] ")[0].getText(),
                                 teamEFG_Percentage=tfoots[1].select(" tr td[data-stat='efg_pct'] ")[0].getText(),
                                 teamOREB_Percentage=tfoots[1].select(" tr td[data-stat='orb_pct'] ")[0].getText(),
                                 teamDREB_Percentage=tfoots[1].select(" tr td[data-stat='drb_pct'] ")[0].getText(),
                                 teamTO_Percentage=tfoots[1].select(" tr td[data-stat='tov_pct'] ")[0].getText(),
                                 teamSTL_Percentage=tfoots[1].select(" tr td[data-stat='stl_pct'] ")[0].getText(),
                                 teamBLK_Percentage=tfoots[1].select(" tr td[data-stat='blk_pct'] ")[0].getText(),
                                 teamBLKR='',
                                 teamPPS='',
                                 teamFIC='',
                                 teamFIC40='',
                                 teamOrtg=tfoots[1].select(" tr td[data-stat='off_rtg'] ")[0].getText(),
                                 teamDrtg=tfoots[1].select(" tr td[data-stat='def_rtg'] ")[0].getText(),
                                 teamEDiff='',
                                 teamPlay_Percentage='',
                                 teamAR='',
                                 teamASTDividedByTO=round(int(tfoots[0].select(" tr td[data-stat='ast'] ")[0].getText())/int(tfoots[0].select(" tr td[data-stat='tov'] ")[0].getText()),2),
                                 teamSTLDividedByTO=round(int(tfoots[0].select(" tr td[data-stat='stl'] ")[0].getText())/int(tfoots[0].select(" tr td[data-stat='tov'] ")[0].getText())*100,2),
                                 teamFouls=tfoots[0].select(" tr td[data-stat='pf'] ")[0].getText(),

                                 opptAbbr=MatchEntry['home'],
                                 opptConf='',opptLoc='Home',opptRslt='Win' if int(MatchEntry['home_score']) > int(MatchEntry['away_score']) else 'Loss',
                                 opptMin=tfoots[2].select(" tr td[data-stat='mp'] ")[0].getText(),
                                 opptDayOff='',
                                 opptPTS=MatchEntry['home_score'],
                                 opptAST=tfoots[2].select(" tr td[data-stat='ast'] ")[0].getText(),
                                 opptTO=tfoots[2].select(" tr td[data-stat='tov'] ")[0].getText(),
                                 opptSTL=tfoots[2].select(" tr td[data-stat='stl'] ")[0].getText(),
                                 opptBLK=tfoots[2].select(" tr td[data-stat='blk'] ")[0].getText(),
                                 opptFGA=tfoots[2].select(" tr td[data-stat='fga'] ")[0].getText(),
                                 opptFGM=tfoots[2].select(" tr td[data-stat='fg'] ")[0].getText(),
                                 opptFG_Percentage=tfoots[2].select(" tr td[data-stat='fg_pct'] ")[0].getText(),
                                 oppt2PA='',
                                 oppt2PM='',
                                 oppt2P_Percentage='',
                                 oppt3PA=tfoots[2].select(" tr td[data-stat='fg3a'] ")[0].getText(),
                                 oppt3PM=tfoots[2].select(" tr td[data-stat='fg3'] ")[0].getText(),
                                 oppt3P_Percentage=tfoots[2].select(" tr td[data-stat='fg3_pct'] ")[0].getText(),
                                 opptFTA=tfoots[2].select(" tr td[data-stat='fta'] ")[0].getText(),
                                 opptFTM=tfoots[2].select(" tr td[data-stat='ft'] ")[0].getText(),
                                 opptFT_Percentage=tfoots[2].select(" tr td[data-stat='ft_pct'] ")[0].getText(),
                                 opptORB=tfoots[2].select(" tr td[data-stat='orb'] ")[0].getText(),
                                 opptDRB=tfoots[2].select(" tr td[data-stat='drb'] ")[0].getText(),
                                 opptTRB=tfoots[2].select(" tr td[data-stat='trb'] ")[0].getText(),
                                 opptPTS1='',
                                 opptPTS2='',
                                 opptPTS3='',
                                 opptPTS4='',
                                 opptPTS5='',
                                 opptPTS6='',
                                 opptPTS7='',
                                 opptPTS8='',
                                 opptTREB_Percentage=tfoots[3].select(" tr td[data-stat='trb_pct'] ")[0].getText(),
                                 opptASST_Percentage=tfoots[3].select(" tr td[data-stat='ast_pct'] ")[0].getText(),
                                 opptTS_Percentage=tfoots[3].select(" tr td[data-stat='ts_pct'] ")[0].getText(),
                                 opptEFG_Percentage=tfoots[3].select(" tr td[data-stat='efg_pct'] ")[0].getText(),
                                 opptOREB_Percentage=tfoots[3].select(" tr td[data-stat='orb_pct'] ")[0].getText(),
                                 opptDREB_Percentage=tfoots[3].select(" tr td[data-stat='drb_pct'] ")[0].getText(),
                                 opptTO_Percentage=tfoots[3].select(" tr td[data-stat='tov_pct'] ")[0].getText(),
                                 opptSTL_Percentage=tfoots[3].select(" tr td[data-stat='stl_pct'] ")[0].getText(),
                                 opptBLK_Percentage=tfoots[3].select(" tr td[data-stat='blk_pct'] ")[0].getText(),
                                 opptBLKR='',
                                 opptPPS='',
                                 opptFIC='',
                                 opptFIC40='',
                                 opptOrtg=tfoots[3].select(" tr td[data-stat='off_rtg'] ")[0].getText(),
                                 opptDrtg=tfoots[3].select(" tr td[data-stat='def_rtg'] ")[0].getText(),
                                 opptEDiff='',
                                 opptPlay_Percentage='',
                                 opptAR='',
                                 opptASTDividedByTO=round(int(tfoots[2].select(" tr td[data-stat='ast'] ")[0].getText())/int(tfoots[2].select(" tr td[data-stat='tov'] ")[0].getText()),2),
                                 opptSTLDividedByTO=round(int(tfoots[2].select(" tr td[data-stat='stl'] ")[0].getText())/int(tfoots[2].select(" tr td[data-stat='tov'] ")[0].getText())*100,2),
                                 opptFouls=tfoots[2].select(" tr td[data-stat='pf'] ")[0].getText())


            TeamGameStats = dict(gmDate=process_DataTime(MatchEntry['date'], MatchEntry['time']).strftime('%Y/%m/%d'),
                                     gmTime=process_DataTime(MatchEntry['date'], MatchEntry['time']).strftime('%H:%M'),
                                     seasType='Regular',
                                     offLNm1='', offFNm1='',
                                     offLNm2='', offFNm2='',
                                     OffLNm3='', offFNm3='',
                                     teamAbbr=MatchEntry['home'],
                                     teamConf='', teamLoc='Home',
                                     teamRslt='Win' if int(MatchEntry['home_score']) > int(MatchEntry['away_score']) else 'Loss',
                                     teamMin=tfoots[2].select(" tr td[data-stat='mp'] ")[0].getText(),
                                     teamDayOff='',
                                     teamPTS=MatchEntry['home_score'],
                                     teamAST=tfoots[2].select(" tr td[data-stat='ast'] ")[0].getText(),
                                     teamTO=tfoots[2].select(" tr td[data-stat='tov'] ")[0].getText(),
                                     teamSTL=tfoots[2].select(" tr td[data-stat='stl'] ")[0].getText(),
                                     teamBLK=tfoots[2].select(" tr td[data-stat='blk'] ")[0].getText(),
                                     teamFGA=tfoots[2].select(" tr td[data-stat='fga'] ")[0].getText(),
                                     teamFGM=tfoots[2].select(" tr td[data-stat='fg'] ")[0].getText(),
                                     teamFG_Percentage=tfoots[2].select(" tr td[data-stat='fg_pct'] ")[0].getText(),
                                     team2PA='',
                                     team2PM='',
                                     team2P_Percentage='',
                                     team3PA=tfoots[2].select(" tr td[data-stat='fg3a'] ")[0].getText(),
                                     team3PM=tfoots[2].select(" tr td[data-stat='fg3'] ")[0].getText(),
                                     team3P_Percentage=tfoots[2].select(" tr td[data-stat='fg3_pct'] ")[0].getText(),
                                     teamFTA=tfoots[2].select(" tr td[data-stat='fta'] ")[0].getText(),
                                     teamFTM=tfoots[2].select(" tr td[data-stat='ft'] ")[0].getText(),
                                     teamFT_Percentage=tfoots[2].select(" tr td[data-stat='ft_pct'] ")[0].getText(),
                                     teamORB=tfoots[2].select(" tr td[data-stat='orb'] ")[0].getText(),
                                     teamDRB=tfoots[2].select(" tr td[data-stat='drb'] ")[0].getText(),
                                     teamTRB=tfoots[2].select(" tr td[data-stat='trb'] ")[0].getText(),
                                     teamPTS1='',
                                     teamPTS2='',
                                     teamPTS3='',
                                     teamPTS4='',
                                     teamPTS5='',
                                     teamPTS6='',
                                     teamPTS7='',
                                     teamPTS8='',
                                     teamTREB_Percentage=tfoots[3].select(" tr td[data-stat='trb_pct'] ")[0].getText(),
                                     teamASST_Percentage=tfoots[3].select(" tr td[data-stat='ast_pct'] ")[0].getText(),
                                     teamTS_Percentage=tfoots[3].select(" tr td[data-stat='ts_pct'] ")[0].getText(),
                                     teamEFG_Percentage=tfoots[3].select(" tr td[data-stat='efg_pct'] ")[0].getText(),
                                     teamOREB_Percentage=tfoots[3].select(" tr td[data-stat='orb_pct'] ")[0].getText(),
                                     teamDREB_Percentage=tfoots[3].select(" tr td[data-stat='drb_pct'] ")[0].getText(),
                                     teamTO_Percentage=tfoots[3].select(" tr td[data-stat='tov_pct'] ")[0].getText(),
                                     teamSTL_Percentage=tfoots[3].select(" tr td[data-stat='stl_pct'] ")[0].getText(),
                                     teamBLK_Percentage=tfoots[3].select(" tr td[data-stat='blk_pct'] ")[0].getText(),
                                     teamBLKR='',
                                     teamPPS='',
                                     teamFIC='',
                                     teamFIC40='',
                                     teamOrtg=tfoots[3].select(" tr td[data-stat='off_rtg'] ")[0].getText(),
                                     teamDrtg=tfoots[3].select(" tr td[data-stat='def_rtg'] ")[0].getText(),
                                     teamEDiff='',
                                     teamPlay_Percentage='',
                                     teamAR='',
                                     teamASTDividedByTO=round(
                                         int(tfoots[2].select(" tr td[data-stat='ast'] ")[0].getText()) / int(
                                             tfoots[2].select(" tr td[data-stat='tov'] ")[0].getText()), 2),
                                     teamSTLDividedByTO=round(
                                         int(tfoots[2].select(" tr td[data-stat='stl'] ")[0].getText()) / int(
                                             tfoots[2].select(" tr td[data-stat='tov'] ")[0].getText()) * 100, 2),
                                     teamFouls=tfoots[2].select(" tr td[data-stat='pf'] ")[0].getText(),

                                     opptAbbr=MatchEntry['away'],
                                     opptConf='', opptLoc='Away',
                                     opptRslt='Win' if int(MatchEntry['home_score']) < int(MatchEntry['away_score']) else 'Loss',
                                     opptMin=tfoots[0].select(" tr td[data-stat='mp'] ")[0].getText(),
                                     opptDayOff='',
                                     opptPTS=MatchEntry['away_score'],
                                     opptAST=tfoots[0].select(" tr td[data-stat='ast'] ")[0].getText(),
                                     opptTO=tfoots[0].select(" tr td[data-stat='tov'] ")[0].getText(),
                                     opptSTL=tfoots[0].select(" tr td[data-stat='stl'] ")[0].getText(),
                                     opptBLK=tfoots[0].select(" tr td[data-stat='blk'] ")[0].getText(),
                                     opptFGA=tfoots[0].select(" tr td[data-stat='fga'] ")[0].getText(),
                                     opptFGM=tfoots[0].select(" tr td[data-stat='fg'] ")[0].getText(),
                                     opptFG_Percentage=tfoots[0].select(" tr td[data-stat='fg_pct'] ")[0].getText(),
                                     oppt2PA='',
                                     oppt2PM='',
                                     oppt2P_Percentage='',
                                     oppt3PA=tfoots[0].select(" tr td[data-stat='fg3a'] ")[0].getText(),
                                     oppt3PM=tfoots[0].select(" tr td[data-stat='fg3'] ")[0].getText(),
                                     oppt3P_Percentage=tfoots[0].select(" tr td[data-stat='fg3_pct'] ")[0].getText(),
                                     opptFTA=tfoots[0].select(" tr td[data-stat='fta'] ")[0].getText(),
                                     opptFTM=tfoots[0].select(" tr td[data-stat='ft'] ")[0].getText(),
                                     opptFT_Percentage=tfoots[0].select(" tr td[data-stat='ft_pct'] ")[0].getText(),
                                     opptORB=tfoots[0].select(" tr td[data-stat='orb'] ")[0].getText(),
                                     opptDRB=tfoots[0].select(" tr td[data-stat='drb'] ")[0].getText(),
                                     opptTRB=tfoots[0].select(" tr td[data-stat='trb'] ")[0].getText(),
                                     opptPTS1='',
                                     opptPTS2='',
                                     opptPTS3='',
                                     opptPTS4='',
                                     opptPTS5='',
                                     opptPTS6='',
                                     opptPTS7='',
                                     opptPTS8='',
                                     opptTREB_Percentage=tfoots[1].select(" tr td[data-stat='trb_pct'] ")[0].getText(),
                                     opptASST_Percentage=tfoots[1].select(" tr td[data-stat='ast_pct'] ")[0].getText(),
                                     opptTS_Percentage=tfoots[1].select(" tr td[data-stat='ts_pct'] ")[0].getText(),
                                     opptEFG_Percentage=tfoots[1].select(" tr td[data-stat='efg_pct'] ")[0].getText(),
                                     opptOREB_Percentage=tfoots[1].select(" tr td[data-stat='orb_pct'] ")[0].getText(),
                                     opptDREB_Percentage=tfoots[1].select(" tr td[data-stat='drb_pct'] ")[0].getText(),
                                     opptTO_Percentage=tfoots[1].select(" tr td[data-stat='tov_pct'] ")[0].getText(),
                                     opptSTL_Percentage=tfoots[1].select(" tr td[data-stat='stl_pct'] ")[0].getText(),
                                     opptBLK_Percentage=tfoots[1].select(" tr td[data-stat='blk_pct'] ")[0].getText(),
                                     opptBLKR='',
                                     opptPPS='',
                                     opptFIC='',
                                     opptFIC40='',
                                     opptOrtg=tfoots[1].select(" tr td[data-stat='off_rtg'] ")[0].getText(),
                                     opptDrtg=tfoots[1].select(" tr td[data-stat='def_rtg'] ")[0].getText(),
                                     opptEDiff='',
                                     opptPlay_Percentage='',
                                     opptAR='',
                                     opptASTDividedByTO=round(
                                         int(tfoots[0].select(" tr td[data-stat='ast'] ")[0].getText()) / int(
                                             tfoots[0].select(" tr td[data-stat='tov'] ")[0].getText()), 2),
                                     opptSTLDividedByTO=round(
                                         int(tfoots[0].select(" tr td[data-stat='stl'] ")[0].getText()) / int(
                                             tfoots[0].select(" tr td[data-stat='tov'] ")[0].getText()) * 100, 2),
                                     opptFouls=tfoots[0].select(" tr td[data-stat='pf'] ")[0].getText())
            with s_print_lock:
                print('OpptGameStats --> ', OpptGameStats,'\n','TeamGameStats --> ',TeamGameStats)
            GameStats[year].append(OpptGameStats)
            GameStats[year].append(TeamGameStats)
            if idx%50 == 0:
                pd.DataFrame(GameStats[year]).to_csv(f'{year}_TeamGameStats.csv', index=False)
            elif idx + 1 == len(MatchEntrys[year]):
                pd.DataFrame(GameStats[year]).to_csv(f'{year}_TeamGameStats.csv', index=False)

        except selenium.common.exceptions.NoSuchElementException:
            with s_print_lock:
                print('找不到值.')
        except:
            traceback.print_exc()


    browser.close()
    browser.quit()


def get_MatchEntrys(year):

    useragent = "Mozilla/5.0 (Linux; Android 8.0.0; Pixel 2 XL Build/OPD1.170816.004) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.87 Mobile Safari/537.36"
    profile = webdriver.FirefoxProfile()
    profile.set_preference("general.useragent.override", useragent)
    options = webdriver.FirefoxOptions()
    options.set_preference("dom.webnotifications.serviceworker.enabled", False)
    options.set_preference("dom.webnotifications.enabled", False)
    options.add_argument('--headless')
    browser = webdriver.Firefox(firefox_profile=profile, options=options)
    with s_print_lock:
        print('*' * 70)
        print('*' * 30, year,'RUN MAIN', '*' * 30)
        print('*' * 70)

    isPlayoffs=0
    for month in ['october','november','december','january','february','march','april']:#,'november','december','january','february','march','april'
        url = f'https://www.basketball-reference.com/leagues/NBA_{year+1}_games-{month}.html'
        with s_print_lock:
            print('*' * 30,url,'*' * 30)
        browser.get(url)
        sleep(1)
        for tr in browser.find_elements_by_css_selector("#schedule tbody tr"):
            try:
                if tr.find_element_by_css_selector('th').text == 'Playoffs':
                    isPlayoffs=1
                    break
                else:
                    with s_print_lock:
                        print('-'*30)
                    MatchEntry = dict(date = tr.find_element_by_css_selector('th[data-stat="date_game"]').text,
                                time = tr.find_element_by_css_selector('td[data-stat="game_start_time"]').text,
                                away = tr.find_element_by_css_selector('td[data-stat="visitor_team_name"]').text,
                                away_score = tr.find_element_by_css_selector('td[data-stat="visitor_pts"]').text,
                                home = tr.find_element_by_css_selector('td[data-stat="home_team_name"]').text,
                                home_score = tr.find_element_by_css_selector('td[data-stat="home_pts"]').text,
                                box_score_url = tr.find_element_by_css_selector('td[data-stat="box_score_text"] a').get_attribute('href'))
                    with s_print_lock:
                        print('MatchEntry --> ',MatchEntry)
                    MatchEntrys[year].append(MatchEntry)
            except selenium.common.exceptions.NoSuchElementException:
                with s_print_lock:
                    print('error field.')
            except:
                traceback.print_exc()
        if isPlayoffs:
            break

    browser.close()
    browser.quit()
    get_TeamGameStats(year)

def main():
    tsk = []
    start_year = 2018
    end_year = 2021

    for year in range(start_year,end_year+1):
        MatchEntrys[year] = []
        GameStats[year] = []
        t = threading.Thread(target=get_MatchEntrys, args=(year,))
        t.start()
        tsk.append(t)

    for t in tsk:
        t.join()

if __name__ == '__main__':
    # main()
    get_MatchEntrysFromDateYesterday()