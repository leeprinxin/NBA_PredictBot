import pandas as pd
import requests,bs4
import json
import os
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

MatchEntrys = {}
GameOdds = {}
TeamNameENToCN_dict = { '亚特兰大老鹰':'Atlanta Hawks','布鲁克林篮网':'Brooklyn Nets','波士顿凯尔特人':'Boston Celtics',
                        '夏洛特黄蜂':'Charlotte Hornets','芝加哥公牛':'Chicago Bulls','克里夫兰骑士':'Cleveland Cavaliers',
                        '达拉斯独行侠':'Dallas Mavericks','丹佛掘金':'Denver Nuggets','底特律活塞':'Detroit Pistons',
                        '金州勇士':'Golden State Warriors','休斯顿火箭':'Houston Rockets','印第安纳步行者':'Indiana Pacers',
                        '洛杉矶快船':'Los Angeles Clippers','洛杉矶湖人':'Los Angeles Lakers','孟菲斯灰熊':'Memphis Grizzlies',
                        '迈阿密热火':'Miami Heat','密尔沃基雄鹿':'Milwaukee Bucks','明尼苏达森林狼':'Minnesota Timberwolves',
                        '新奥尔良鹈鹕':'New Orleans Pelicans','纽约尼克斯':'New York Knicks','俄克拉荷马城雷霆':'Oklahoma City Thunder',
                        '奥兰多魔术':'Orlando Magic','费城76人':'Philadelphia 76ers','菲尼克斯太阳':'Phoenix Suns',
                        '波特兰开拓者':'Portland Trail Blazers','圣安东尼奥马刺':'San Antonio Spurs','萨克拉门托国王':'Sacramento Kings',
                        '多伦多猛龙':'Toronto Raptors','犹他爵士':'Utah Jazz','华盛顿奇才':'Washington Wizards'}
s_print_lock = Lock()

def MonthNumberToText(num):
    NumberToText = {10:'october', 11:'november', 12:'december', 1:'january', 2:'february', 3:'march', 4:'april', 5:'may',6:'june', 7:'july'}
    return NumberToText[num]

def get_TeamGameOddsFromNotStarted(TWDateTimeTop,TWDateTimeBottom,TW_year,TW_month):

    print(datetime.now().strftime('%Y/%m/%d %H:%M:%S'), '-' * 10, f"RUN {TW_year}-{TW_year + 1} get_TeamGameOdds (TW. {TWDateTimeTop.strftime('%Y/%m/%d %H:%M')}-{TWDateTimeBottom.strftime('%Y/%m/%d %H:%M')})")
    if MatchEntrys == {}:
        print(datetime.now().strftime('%Y/%m/%d %H:%M:%S'), '-' * 10, f'{TW_year}-{TW_year + 1} MatchEntrys尚未爬取')

    useragent = "Mozilla/5.0 (Linux; Android 8.0.0; Pixel 2 XL Build/OPD1.170816.004) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.87 Mobile Safari/537.36"
    profile = webdriver.FirefoxProfile()
    profile.set_preference("general.useragent.override", useragent)
    options = webdriver.FirefoxOptions()
    options.set_preference("dom.webnotifications.serviceworker.enabled", False)
    options.set_preference("dom.webnotifications.enabled", False)
    options.add_argument('--headless')
    browser = webdriver.Firefox(firefox_profile=profile, options=options)
    #print(MatchEntrys[TW_year])
    for idx, MatchEntry in enumerate(MatchEntrys[TW_year]):
        try:
            #print(idx, MatchEntry )
            # 爬取強弱盤
            browser.get(MatchEntry['oddslist'])
            sleep(1)
            browser.find_element_by_xpath("//option[@value='1']").click()
            objSoup = bs4.BeautifulSoup(browser.find_element_by_xpath('//*').get_attribute('outerHTML'), 'lxml')

            for tr, tr2 in zip([objSoup.select("tr#oddstr_214")[0]],
                               [objSoup.select("tr#oddstr_214")[0].find_next_sibling()]):
                tds = tr.select('td')
                td2s = tr2.select('td')
                home1x2FirstOptionRate = tds[2].getText().strip()
                away1x2FirstOptionRate = tds[3].getText().strip()
                home1x2LastOptionRate = td2s[0].getText().strip()
                away1x2LastOptionRate = td2s[1].getText().strip()
                home1x2FirstWinRate = tds[4].getText().strip()
                away1x2FirstWinRate = tds[5].getText().strip()
                home1x2LastWinRate = td2s[2].getText().strip()
                away1x2LastWinRate = td2s[3].getText().strip()
                home1x2FirstRTP = tds[6].getText().strip()
                away1x2LastRTP = td2s[4].getText().strip()
                home1x2KellyIndex = tds[7].getText().strip()
                away1x2KellyIndex = tds[8].getText().strip()

            # 爬取大小盤
            browser.find_element_by_xpath("//a[contains(text(),'总分')]").click()
            sleep(1)
            objSoup = bs4.BeautifulSoup(browser.find_element_by_xpath('//*').get_attribute('outerHTML'), 'lxml')
            for tr in objSoup.select("table#odds tbody tr[height='30']"):
                tds = tr.select('td')
                if '365' in tds[0].getText():
                    OverFirstOptionRate = tds[2].getText().strip()
                    UnderFirstOptionRate = tds[4].getText().strip()
                    OverUnderFirstSpecialBetValue = tds[3].getText().strip()
                    OverLastOptionRate = tds[5].getText().strip()
                    UnderLastOptionRate = tds[7].getText().strip()
                    OverUnderLastSpecialBetValue = tds[6].getText().strip()

            # 整理資料
            TeamGameOdds = dict(gmDate=MatchEntry['gmDateTime'].strftime('%Y/%m/%d'),
                                gmTime=MatchEntry['gmDateTime'].strftime('%H:%M'),
                                homeAbbr=MatchEntry['homeAbbr'],
                                awayAbbr=MatchEntry['awayAbbr'],

                                home1x2FirstOptionRate=home1x2FirstOptionRate,
                                away1x2FirstOptionRate=away1x2FirstOptionRate,
                                home1x2LastOptionRate=home1x2LastOptionRate,
                                away1x2LastOptionRate=away1x2LastOptionRate,
                                home1x2FirstWinRate=home1x2FirstWinRate,
                                away1x2FirstWinRate=away1x2FirstWinRate,
                                home1x2LastWinRate=home1x2LastWinRate,
                                away1x2LastWinRate=away1x2LastWinRate,
                                home1x2FirstRTP=home1x2FirstRTP,
                                away1x2LastRTP=away1x2LastRTP,
                                home1x2KellyIndex=home1x2KellyIndex,
                                away1x2KellyIndex=away1x2KellyIndex,

                                OverFirstOptionRate=OverFirstOptionRate,
                                UnderFirstOptionRate=UnderFirstOptionRate,
                                OverUnderFirstSpecialBetValue=OverUnderFirstSpecialBetValue,
                                OverLastOptionRate=OverLastOptionRate,
                                UnderLastOptionRate=UnderLastOptionRate,
                                OverUnderLastSpecialBetValue=OverUnderLastSpecialBetValue,
                                SourceUrl=MatchEntry['oddslist']
                                )

            print(datetime.now().strftime('%Y/%m/%d %H:%M:%S'), '-' * 10,f'爬取 {TW_year}-{TW_year + 1} 賽事賠率 = {TeamGameOdds}')
            GameOdds[TW_year].append(TeamGameOdds)

        except:
            print(datetime.now().strftime('%Y/%m/%d %H:%M:%S'), '-' * 10, f'({MatchEntry["oddslist"]}) ERROR.')
            traceback.print_exc()

    browser.close()
    browser.quit()


    return pd.DataFrame(GameOdds[TW_year])



def get_MatchEntrysFromNotStarted():
    # 得到台灣時間
    TWDateTime = datetime.now()
    # 設定範圍
    TWDateTimeTop, TWDateTimeBottom = (TWDateTime-timedelta(days=0)).replace(microsecond=0), (TWDateTime+timedelta(days=1)).replace(hour=23, minute=59, second=59, microsecond=0)
    #TWDateTimeTop, TWDateTimeBottom = (TWDateTime - timedelta(days=0)).replace(hour=0, minute=0, second=0, microsecond=0), (TWDateTime + timedelta(days=1)).replace(hour=23, minute=59, second=59, microsecond=0)
    TW_month = TWDateTimeTop.month
    TW_year = TWDateTimeTop.year - 1 if TW_month < 10 else TWDateTimeTop.year
    MatchEntrys[TW_year] = []
    GameOdds[TW_year] = []

    useragent = "Mozilla/5.0 (Linux; Android 8.0.0; Pixel 2 XL Build/OPD1.170816.004) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.87 Mobile Safari/537.36"
    profile = webdriver.FirefoxProfile()
    profile.set_preference("general.useragent.override", useragent)
    options = webdriver.FirefoxOptions()
    options.set_preference("dom.webnotifications.serviceworker.enabled", False)
    options.set_preference("dom.webnotifications.enabled", False)
    options.add_argument('--headless')
    browser = webdriver.Firefox(firefox_profile=profile, options=options)

    url = f'https://nba.win007.com/cn/Normal.aspx?y={TW_year + 1 if TW_month < 10 else TW_year}&m={TW_month}&matchSeason={TW_year}-{TW_year + 1}&SclassID=1'
    print(datetime.now().strftime('%Y/%m/%d %H:%M:%S'), '-' * 10,
          f"RUN {TW_year}-{TW_year + 1} MatchEntrys (TW. {TWDateTimeTop.strftime('%Y/%m/%d %H:%M')}-{TWDateTimeBottom.strftime('%Y/%m/%d %H:%M')})({url})")
    print(datetime.now().strftime('%Y/%m/%d %H:%M:%S'), '-' * 10,
          f'爬取 {TW_year + 1 if TW_month < 10 else TW_year}年{TW_month}月 ({url}) 賽事表')
    try:
        browser.get(url)
        sleep(1)
        objSoup = bs4.BeautifulSoup(browser.find_element_by_xpath('//*').get_attribute('outerHTML'), 'lxml')
        for tr in objSoup.select("div#scheDiv table tbody tr[align='center'] "):
            try:
                tds = tr.select('td')
                MatchEntry = dict(seasType=tds[0].getText(),
                                  gmDateTime=datetime.strptime(
                                      f'{TW_year + 1 if TW_month < 10 else TW_year}-' + tds[1].getText(), '%Y-%m-%d %H:%M'),
                                  homeAbbr=TeamNameENToCN_dict[tds[2].getText()],
                                  awayAbbr=TeamNameENToCN_dict[tds[4].getText()],
                                  analysis='http://nba.win007.com' + tds[7].select('a')[0].get('href'),
                                  oddslist='http://nba.win007.com' + tds[7].select('a')[1].get('href'))
                if MatchEntry['gmDateTime'] >= TWDateTimeTop and MatchEntry['gmDateTime'] <= TWDateTimeBottom:
                    print(datetime.now().strftime('%Y/%m/%d %H:%M:%S'), '-' * 10,
                          f'爬取 {TW_year + 1 if TW_month < 10 else TW_year}年{TW_month}月 賽事 = {MatchEntry}')
                    MatchEntrys[TW_year].append(MatchEntry)
            except:
                print(datetime.now().strftime('%Y/%m/%d %H:%M:%S'), '-' * 10, f'({tr.getText()}) ERROR.')
    except:
        traceback.print_exc()

    url = f'http://nba.win007.com/cn/Playoffs.aspx?SclassID=1&matchSeason={TW_year}-{TW_year + 1}'
    print(datetime.now().strftime('%Y/%m/%d %H:%M:%S'), '-' * 10,
          f"RUN {TW_year}-{TW_year + 1} MatchEntrys Playoffs(TW. {TWDateTimeTop.strftime('%Y/%m/%d %H:%M')}-{TWDateTimeBottom.strftime('%Y/%m/%d %H:%M')})({url})")
    print(datetime.now().strftime('%Y/%m/%d %H:%M:%S'), '-' * 10,
          f'爬取 {TW_year + 1 if TW_month < 10 else TW_year}年{TW_month}月 ({url}) 季後賽事表')
    try:
        browser.get(url)
        sleep(1)
        for cupmatch in ['东部第一圈', '西部第一圈', '东部第二圈', '西部第二圈', '东部决赛', '西部决赛', '总决赛']:
            browser.find_element_by_xpath(f"//td[contains(text(),'{cupmatch}')]").click()
            sleep(1)
            objSoup = bs4.BeautifulSoup(browser.find_element_by_xpath('//*').get_attribute('outerHTML'), 'lxml')
            for tr in objSoup.select("div#scheDiv table tbody tr[align='center'] "):
                try:
                    if len(tr.attrs) >= 2:
                        tds = tr.select('td')
                        MatchEntry = dict(seasType=tds[0].getText(),
                                          gmDateTime=datetime.strptime(
                                              f'{TW_year + 1 if TW_month < 10 else TW_year}-' + tds[1].getText(),
                                              '%Y-%m-%d %H:%M'),
                                          homeAbbr=TeamNameENToCN_dict[tds[2].getText()],
                                          awayAbbr=TeamNameENToCN_dict[tds[4].getText()],
                                          analysis='http://nba.win007.com' + tds[7].select('a')[0].get('href'),
                                          oddslist='http://nba.win007.com' + tds[7].select('a')[1].get('href'))

                        if MatchEntry['gmDateTime'] >= TWDateTimeTop and MatchEntry['gmDateTime'] <= TWDateTimeBottom:
                            print(datetime.now().strftime('%Y/%m/%d %H:%M:%S'), '-' * 10,
                                  f'爬取 {TW_year + 1 if TW_month < 10 else TW_year}年{TW_month}月 季後賽事 = {MatchEntry}')
                            MatchEntrys[TW_year].append(MatchEntry)
                except:
                    print(datetime.now().strftime('%Y/%m/%d %H:%M:%S'), '-' * 10, f'({tr.getText()}) Playoffs ERROR.')
    except:
        traceback.print_exc()

    browser.close()
    browser.quit()
    # 爬取 Odds
    return get_TeamGameOddsFromNotStarted(TWDateTimeTop, TWDateTimeBottom, TW_year, TW_month)

def get_TeamGameOddsFromYesterday(TWDateTimeTop,TWDateTimeBottom,TW_year,TW_month):

    print(datetime.now().strftime('%Y/%m/%d %H:%M:%S'), '-' * 10, f"RUN {TW_year}-{TW_year + 1} get_TeamGameOdds (TW. {TWDateTimeTop.strftime('%Y/%m/%d %H:%M')}-{TWDateTimeBottom.strftime('%Y/%m/%d %H:%M')})")
    if MatchEntrys == {}:
        print(datetime.now().strftime('%Y/%m/%d %H:%M:%S'), '-' * 10, f'{TW_year}-{TW_year + 1} MatchEntrys尚未爬取')

    useragent = "Mozilla/5.0 (Linux; Android 8.0.0; Pixel 2 XL Build/OPD1.170816.004) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.87 Mobile Safari/537.36"
    profile = webdriver.FirefoxProfile()
    profile.set_preference("general.useragent.override", useragent)

    '''profile.set_preference('network.proxy.type', 1)
    profile.set_preference('network.proxy.http', '164.70.77.211')
    profile.set_preference('network.proxy.http_port', 3128)  # int'''

    options = webdriver.FirefoxOptions()
    options.set_preference("dom.webnotifications.serviceworker.enabled", False)
    options.set_preference("dom.webnotifications.enabled", False)
    options.add_argument('--headless')
    browser = webdriver.Firefox(firefox_profile=profile, options=options)
    for idx, MatchEntry in enumerate(MatchEntrys[TW_year]):
        try:
            # 爬取強弱盤
            browser.get(MatchEntry['oddslist'])
            sleep(1)
            browser.find_element_by_xpath("//option[@value='1']").click()
            objSoup = bs4.BeautifulSoup(browser.find_element_by_xpath('//*').get_attribute('outerHTML'), 'lxml')

            for tr, tr2 in zip([objSoup.select("tr#oddstr_214")[0]],
                               [objSoup.select("tr#oddstr_214")[0].find_next_sibling()]):
                tds = tr.select('td')
                td2s = tr2.select('td')
                home1x2FirstOptionRate = tds[2].getText().strip()
                away1x2FirstOptionRate = tds[3].getText().strip()
                home1x2LastOptionRate = td2s[0].getText().strip()
                away1x2LastOptionRate = td2s[1].getText().strip()
                home1x2FirstWinRate = tds[4].getText().strip()
                away1x2FirstWinRate = tds[5].getText().strip()
                home1x2LastWinRate = td2s[2].getText().strip()
                away1x2LastWinRate = td2s[3].getText().strip()
                home1x2FirstRTP = tds[6].getText().strip()
                away1x2LastRTP = td2s[4].getText().strip()
                home1x2KellyIndex = tds[7].getText().strip()
                away1x2KellyIndex = tds[8].getText().strip()

            # 爬取大小盤
            browser.find_element_by_xpath("//a[contains(text(),'总分')]").click()
            sleep(1)
            objSoup = bs4.BeautifulSoup(browser.find_element_by_xpath('//*').get_attribute('outerHTML'), 'lxml')
            for tr in objSoup.select("table#odds tbody tr[height='30']"):
                tds = tr.select('td')
                if '365' in tds[0].getText():
                    OverFirstOptionRate = tds[2].getText().strip()
                    UnderFirstOptionRate = tds[4].getText().strip()
                    OverUnderFirstSpecialBetValue = tds[3].getText().strip()
                    OverLastOptionRate = tds[5].getText().strip()
                    UnderLastOptionRate = tds[7].getText().strip()
                    OverUnderLastSpecialBetValue = tds[6].getText().strip()

            # 整理資料
            TeamGameOdds = dict(gmDate=MatchEntry['gmDateTime'].strftime('%Y/%m/%d'),
                                gmTime=MatchEntry['gmDateTime'].strftime('%H:%M'),
                                homeAbbr=MatchEntry['homeAbbr'],
                                awayAbbr=MatchEntry['awayAbbr'],

                                home1x2FirstOptionRate=home1x2FirstOptionRate,
                                away1x2FirstOptionRate=away1x2FirstOptionRate,
                                home1x2LastOptionRate=home1x2LastOptionRate,
                                away1x2LastOptionRate=away1x2LastOptionRate,
                                home1x2FirstWinRate=home1x2FirstWinRate,
                                away1x2FirstWinRate=away1x2FirstWinRate,
                                home1x2LastWinRate=home1x2LastWinRate,
                                away1x2LastWinRate=away1x2LastWinRate,
                                home1x2FirstRTP=home1x2FirstRTP,
                                away1x2LastRTP=away1x2LastRTP,
                                home1x2KellyIndex=home1x2KellyIndex,
                                away1x2KellyIndex=away1x2KellyIndex,

                                OverFirstOptionRate=OverFirstOptionRate,
                                UnderFirstOptionRate=UnderFirstOptionRate,
                                OverUnderFirstSpecialBetValue=OverUnderFirstSpecialBetValue,
                                OverLastOptionRate=OverLastOptionRate,
                                UnderLastOptionRate=UnderLastOptionRate,
                                OverUnderLastSpecialBetValue=OverUnderLastSpecialBetValue,
                                SourceUrl=MatchEntry['oddslist']
                                )

            print(datetime.now().strftime('%Y/%m/%d %H:%M:%S'), '-' * 10,f'爬取 {TW_year}-{TW_year + 1} 賽事賠率 = {TeamGameOdds}')
            GameOdds[TW_year].append(TeamGameOdds)

        except:
            print(datetime.now().strftime('%Y/%m/%d %H:%M:%S'), '-' * 10, f'({MatchEntry["oddslist"]}) ERROR.')
            traceback.print_exc()

        if idx + 1 == len(MatchEntrys[TW_year]):
            filepath = f'C:/python_projects_temp/NBA_Predict/NBA_DATA3-5/{TW_year}_TeamGameOdds.csv'
            print(datetime.now().strftime('%Y/%m/%d %H:%M:%S'), '-' * 10,f'{TW_year}-{TW_year + 1} 賽事賠率寫入 --> {filepath} ')
            if os.path.isfile(filepath):
                pd.concat([pd.read_csv(filepath), pd.DataFrame(GameOdds[TW_year])], ignore_index=True).drop_duplicates(subset=['gmDate', 'gmTime', 'homeAbbr', 'awayAbbr'], keep='last').to_csv(filepath, header=True,index=False)
            else:
                pd.DataFrame(GameOdds[TW_year]).to_csv(filepath, header=True, index=False)
    browser.close()
    browser.quit()

def get_MatchEntrysFromYesterday():
    # 得到台灣時間
    TWDateTime = datetime.now()-timedelta(days=1)
    # 設定範圍
    TWDateTimeTop, TWDateTimeBottom = TWDateTime.replace(hour=0, minute=0, second=0, microsecond=0),TWDateTime.replace(hour=23, minute=59, second=59, microsecond=0)

    TW_month = TWDateTimeTop.month
    TW_year =  TWDateTimeTop.year-1 if TW_month < 10 else TWDateTimeTop.year
    MatchEntrys[TW_year] = []
    GameOdds[TW_year] = []

    useragent = "Mozilla/5.0 (Linux; Android 8.0.0; Pixel 2 XL Build/OPD1.170816.004) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.87 Mobile Safari/537.36"
    profile = webdriver.FirefoxProfile()
    profile.set_preference("general.useragent.override", useragent)
    '''profile.set_preference('network.proxy.type', 1)
    profile.set_preference('network.proxy.http', '164.70.77.211')
    profile.set_preference('network.proxy.http_port', 3128)  # int'''
    options = webdriver.FirefoxOptions()
    options.set_preference("dom.webnotifications.serviceworker.enabled", False)
    options.set_preference("dom.webnotifications.enabled", False)
    options.add_argument('--headless')
    browser = webdriver.Firefox(firefox_profile=profile, options=options)

    url = f'https://nba.win007.com/cn/Normal.aspx?y={TW_year + 1 if TW_month < 10 else TW_year}&m={TW_month}&matchSeason={TW_year}-{TW_year + 1}&SclassID=1'
    print(datetime.now().strftime('%Y/%m/%d %H:%M:%S'),'-' * 10,f"RUN {TW_year}-{TW_year+1} MatchEntrys (TW. {TWDateTimeTop.strftime('%Y/%m/%d %H:%M')}-{TWDateTimeBottom.strftime('%Y/%m/%d %H:%M')})({url})")
    print(datetime.now().strftime('%Y/%m/%d %H:%M:%S'),'-' * 10,f'爬取 {TW_year+1 if TW_month<10 else TW_year}年{TW_month}月 ({url}) 賽事表')
    try:
        browser.get(url)
        sleep(1)
        objSoup = bs4.BeautifulSoup(browser.find_element_by_xpath('//*').get_attribute('outerHTML'), 'lxml')
        for tr in objSoup.select("div#scheDiv table tbody tr[align='center'] "):

                tds = tr.select('td')
                MatchEntry = dict(seasType = tds[0].getText(),
                                  gmDateTime = datetime.strptime(f'{TW_year+1 if TW_month<10 else TW_year}-'+tds[1].getText(),'%Y-%m-%d %H:%M'),
                                  homeAbbr = TeamNameENToCN_dict[tds[2].getText()],
                                  awayAbbr = TeamNameENToCN_dict[tds[4].getText()],
                                  analysis = 'http://nba.win007.com'+tds[7].select('a')[0].get('href'),
                                  oddslist = 'http://nba.win007.com'+tds[7].select('a')[1].get('href'))
                if MatchEntry['gmDateTime' ]>= TWDateTimeTop and MatchEntry['gmDateTime' ]<= TWDateTimeBottom:
                    print(datetime.now().strftime('%Y/%m/%d %H:%M:%S'),'-' * 10,f'爬取 {TW_year+1 if TW_month<10 else TW_year}年{TW_month}月 賽事 = {MatchEntry}' )
                    MatchEntrys[TW_year].append(MatchEntry)
    except:
        print(datetime.now().strftime('%Y/%m/%d %H:%M:%S'), '-' * 10, f'({url}) ERROR.')
        traceback.print_exc()

    url = f'http://nba.win007.com/cn/Playoffs.aspx?SclassID=1&matchSeason={TW_year}-{TW_year+1}'
    print(datetime.now().strftime('%Y/%m/%d %H:%M:%S'),'-' * 10,f"RUN {TW_year}-{TW_year+1} MatchEntrys Playoffs(TW. {TWDateTimeTop.strftime('%Y/%m/%d %H:%M')}-{TWDateTimeBottom.strftime('%Y/%m/%d %H:%M')})({url})")
    print(datetime.now().strftime('%Y/%m/%d %H:%M:%S'),'-' * 10,f'爬取 {TW_year+1 if TW_month<10 else TW_year}年{TW_month}月 ({url}) 季後賽事表')
    try:
        browser.get(url)
        sleep(1)
        for cupmatch in ['东部第一圈','西部第一圈','东部第二圈','西部第二圈','东部决赛','西部决赛','总决赛']:
            browser.find_element_by_xpath(f"//td[contains(text(),'{cupmatch}')]").click()
            sleep(1)
            objSoup = bs4.BeautifulSoup(browser.find_element_by_xpath('//*').get_attribute('outerHTML'), 'lxml')
            for tr in objSoup.select("div#scheDiv table tbody tr[align='center'] "):
                if len(tr.attrs)>=2:
                    tds = tr.select('td')
                    MatchEntry = dict(seasType = tds[0].getText(),
                                      gmDateTime = datetime.strptime(f'{TW_year+1 if TW_month<10 else TW_year}-'+tds[1].getText(),'%Y-%m-%d %H:%M'),
                                      homeAbbr = TeamNameENToCN_dict[tds[2].getText()],
                                      awayAbbr = TeamNameENToCN_dict[tds[4].getText()],
                                      analysis = 'http://nba.win007.com'+tds[7].select('a')[0].get('href'),
                                      oddslist = 'http://nba.win007.com'+tds[7].select('a')[1].get('href'))

                    if MatchEntry['gmDateTime' ]>= TWDateTimeTop and MatchEntry['gmDateTime' ]<= TWDateTimeBottom:
                        print(datetime.now().strftime('%Y/%m/%d %H:%M:%S'),'-' * 10,f'爬取 {TW_year+1 if TW_month<10 else TW_year}年{TW_month}月 季後賽事 = {MatchEntry}' )
                        MatchEntrys[TW_year].append(MatchEntry)
    except:
        print(datetime.now().strftime('%Y/%m/%d %H:%M:%S'), '-' * 10, f'({url}) Playoffs ERROR.')
        traceback.print_exc()

    browser.close()
    browser.quit()
    # 爬取 Odds
    get_TeamGameOddsFromYesterday(TWDateTimeTop,TWDateTimeBottom,TW_year,TW_month)


def get_TeamGameOdds(year):
    with s_print_lock:
        print(datetime.now().strftime('%Y/%m/%d %H:%M:%S'),'-' * 10, f'RUN {year}-{year+1} get_TeamGameOdds')
    if MatchEntrys == {}:
        with s_print_lock:
            print(datetime.now().strftime('%Y/%m/%d %H:%M:%S'),'-' * 10,f'{year}-{year+1} MatchEntrys尚未爬取')

    useragent = "Mozilla/5.0 (Linux; Android 8.0.0; Pixel 2 XL Build/OPD1.170816.004) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.87 Mobile Safari/537.36"
    profile = webdriver.FirefoxProfile()
    profile.set_preference("general.useragent.override", useragent)
    options = webdriver.FirefoxOptions()
    options.set_preference("dom.webnotifications.serviceworker.enabled", False)
    options.set_preference("dom.webnotifications.enabled", False)
    options.add_argument('--headless')
    browser = webdriver.Firefox(firefox_profile=profile, options=options)
    for idx,MatchEntry in enumerate(MatchEntrys[year]):
        try:
            # 爬取強弱盤
            browser.get(MatchEntry['oddslist'])
            sleep(1)
            browser.find_element_by_xpath("//option[@value='1']").click()
            objSoup = bs4.BeautifulSoup(browser.find_element_by_xpath('//*').get_attribute('outerHTML'), 'lxml')

            for tr,tr2 in zip([objSoup.select("tr#oddstr_214")[0]],[objSoup.select("tr#oddstr_214")[0].find_next_sibling()]):
                tds = tr.select('td')
                td2s = tr2.select('td')
                home1x2FirstOptionRate = tds[2].getText().strip()
                away1x2FirstOptionRate = tds[3].getText().strip()
                home1x2LastOptionRate = td2s[0].getText().strip()
                away1x2LastOptionRate = td2s[1].getText().strip()
                home1x2FirstWinRate = tds[4].getText().strip()
                away1x2FirstWinRate = tds[5].getText().strip()
                home1x2LastWinRate = td2s[2].getText().strip()
                away1x2LastWinRate = td2s[3].getText().strip()
                home1x2FirstRTP = tds[6].getText().strip()
                away1x2LastRTP= td2s[4].getText().strip()
                home1x2KellyIndex= tds[7].getText().strip()
                away1x2KellyIndex = tds[8].getText().strip()

            # 爬取大小盤
            browser.find_element_by_xpath("//a[contains(text(),'总分')]").click()
            sleep(1)
            objSoup = bs4.BeautifulSoup(browser.find_element_by_xpath('//*').get_attribute('outerHTML'), 'lxml')
            for tr in objSoup.select("table#odds tbody tr[height='30']"):
                tds = tr.select('td')
                if '365' in tds[0].getText():
                    OverFirstOptionRate = tds[2].getText().strip()
                    UnderFirstOptionRate = tds[4].getText().strip()
                    OverUnderFirstSpecialBetValue = tds[3].getText().strip()
                    OverLastOptionRate = tds[5].getText().strip()
                    UnderLastOptionRate = tds[7].getText().strip()
                    OverUnderLastSpecialBetValue = tds[6].getText().strip()

            # 整理資料
            TeamGameOdds = dict(gmDate =  MatchEntry['gmDateTime'].strftime('%Y/%m/%d'),
                                gmTime = MatchEntry['gmDateTime'].strftime('%H:%M'),
                                homeAbbr =  MatchEntry['homeAbbr'],
                                awayAbbr =  MatchEntry['awayAbbr'],

                                home1x2FirstOptionRate=home1x2FirstOptionRate,
                                away1x2FirstOptionRate = away1x2FirstOptionRate,
                                home1x2LastOptionRate = home1x2LastOptionRate,
                                away1x2LastOptionRate = away1x2LastOptionRate,
                                home1x2FirstWinRate = home1x2FirstWinRate,
                                away1x2FirstWinRate = away1x2FirstWinRate,
                                home1x2LastWinRate = home1x2LastWinRate,
                                away1x2LastWinRate = away1x2LastWinRate,
                                home1x2FirstRTP = home1x2FirstRTP,
                                away1x2LastRTP = away1x2LastRTP,
                                home1x2KellyIndex = home1x2KellyIndex,
                                away1x2KellyIndex = away1x2KellyIndex,

                                OverFirstOptionRate=OverFirstOptionRate,
                                UnderFirstOptionRate = UnderFirstOptionRate,
                                OverUnderFirstSpecialBetValue = OverUnderFirstSpecialBetValue,
                                OverLastOptionRate = OverLastOptionRate,
                                UnderLastOptionRate = UnderLastOptionRate,
                                OverUnderLastSpecialBetValue = OverUnderLastSpecialBetValue,
                                SourceUrl = MatchEntry['oddslist']
                                )
            with s_print_lock:
                print(datetime.now().strftime('%Y/%m/%d %H:%M:%S'),'-' * 10, f'爬取 {year}-{year+1} 賽事賠率 = {TeamGameOdds}' )
            GameOdds[year].append(TeamGameOdds)
            if (idx+1)%20 == 0:
                with s_print_lock:
                    print(datetime.now().strftime('%Y/%m/%d %H:%M:%S'), '-' * 10,f'{year}-{year+1} 賽事賠率寫入 --> {year}_TeamGameOdds.csv ')
                pd.DataFrame(GameOdds[year]).to_csv(f'{year}_TeamGameOdds.csv', index=False)
            elif idx+1 == len(MatchEntrys[year]):
                with s_print_lock:
                    print(datetime.now().strftime('%Y/%m/%d %H:%M:%S'), '-' * 10,f'{year}-{year+1} 賽事賠率寫入 --> {year}_TeamGameOdds.csv ')
                pd.DataFrame(GameOdds[year]).to_csv(f'{year}_TeamGameOdds.csv', index=False)
        except:
            print(datetime.now().strftime('%Y/%m/%d %H:%M:%S'), '-' * 10, f'({MatchEntry["oddslist"]}) ERROR.')
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
        print(datetime.now().strftime('%Y/%m/%d %H:%M:%S'),'-' * 10,f'RUN {year}-{year+1} MatchEntrys')
    for month in [10,11,12,1,2,3,4]:
        url = f'https://nba.win007.com/cn/Normal.aspx?y={year+1 if month<10 else year}&m={month}&matchSeason={year}-{year+1}&SclassID=1'
        with s_print_lock:
            print(datetime.now().strftime('%Y/%m/%d %H:%M:%S'),'-' * 10,f'爬取 {year+1 if month<10 else year}年{month}月 ({url}) 賽事表')
        try:
            browser.get(url)
            sleep(1)
            objSoup = bs4.BeautifulSoup(browser.find_element_by_xpath('//*').get_attribute('outerHTML'), 'lxml')
            for tr in objSoup.select("div#scheDiv table tbody tr[align='center'] "):

                    tds = tr.select('td')
                    MatchEntry = dict(seasType = tds[0].getText(),
                                      gmDateTime = datetime.strptime(f'{year+1 if month<10 else year}-'+tds[1].getText(),'%Y-%m-%d %H:%M'),
                                      homeAbbr = TeamNameENToCN_dict[tds[2].getText()],
                                      awayAbbr = TeamNameENToCN_dict[tds[4].getText()],
                                      analysis = 'http://nba.win007.com'+tds[7].select('a')[0].get('href'),
                                      oddslist = 'http://nba.win007.com'+tds[7].select('a')[1].get('href'))
                    with s_print_lock:
                        print(datetime.now().strftime('%Y/%m/%d %H:%M:%S'),'-' * 10,f'爬取 {year+1 if month<10 else year}年{month}月 賽事 = {MatchEntry}' )
                    MatchEntrys[year].append(MatchEntry)
        except:
            print(datetime.now().strftime('%Y/%m/%d %H:%M:%S'), '-' * 10, f'({url}) ERROR.')
            traceback.print_exc()

    browser.close()
    browser.quit()
    # 爬取 Odds
    get_TeamGameOdds(year)

def main():

    tsk = []
    start_year = 2014
    end_year = 2021

    for year in range(start_year,end_year+1):
        MatchEntrys[year] = []
        GameOdds[year] = []
        t = threading.Thread(target=get_MatchEntrys, args=(year,))
        t.start()
        tsk.append(t)

    for t in tsk:
        t.join()


if __name__ == '__main__':
    #main()
    get_MatchEntrysFromYesterday()

'''OverFirstOptionRate =
UnderFirstOptionRate =
OverUnderFirstSpecialBetValue =
OverLastOptionRate =
UnderLastOptionRate =
OverUnderLastSpecialBetValue =

# 球探網以客隊讓分值為主
homeHandicapFirstOptionRate =
awayHandicapFirstOptionRate =
homeFirstSpecialBetValue =
awayFirstSpecialBetValue =
homeHandicapLastOptionRate =
awayHandicapLastOptionRate =
homeLastSpecialBetValue =
awayLastSpecialBetValue ='''