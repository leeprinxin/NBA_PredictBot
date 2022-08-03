import pickle, os
import joblib
import pandas as pd
from datetime import datetime, timezone, timedelta
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import math
import pymssql
import warnings
warnings.filterwarnings('ignore')
import pickle
from os import listdir
import os, time
import traceback
import sys
sys.path.append("C:/python_projects_temp/NBA_Predict/NBA_DATA3-5")
import get_TeamGameOdds as get_TeamGameOdds,get_TeamGameStats as get_TeamGameStats,combine_TeamGameStatsAndOdds as combine_TeamGameStatsAndOdds
import argparse
import web_config

class NBAPredict(object):
    def __init__(self):
        self.SourceCode = 'Bet365'
        self.GroupOptionCode = '20'
        self.GroupOptionName = 'Moneyline'
        self.SportCode = 2 # NBA運動編碼: 2 來自SportCode table
        self.SportTournamentCode = '10041830' # 沿用
        self.EventType = 0
        self.CollectClient = 'NBA'
        self.server = web_config.production().server
        self.database = web_config.production().database 
        self.user = web_config.production().username
        self.password = web_config.production().
        self.UserId = '12251514-bb49-490a-ba4d-cdd437b0d651' #koer3745@gmail.com   BruceWayne
        self.TournamentText='NBA'
        self.status = 2
        self.gameType = ['Forecast','Selling']
        self.MarketType = 'international'

    def update_dataset(self):
        print('功能停用...')
        '''get_TeamGameOdds.get_MatchEntrysFromYesterday()
        get_TeamGameStats.get_MatchEntrysFromDateYesterday()
        combine_TeamGameStatsAndOdds.CombineGameFromYesterday()'''

    def add_userbouns(self, predict_num, cursor, db):

        if predict_num > 0:
            Modify_dd = datetime.now().strftime("%Y-%m-%d %H:%M:%S.000")
            start_dd = datetime.now().replace(hour=0, minute=0,second=0,microsecond=0).strftime("%Y-%m-%d %H:%M:%S.000")
            end_dd = datetime.now().replace(hour=23, minute=59,second=59,microsecond=0).strftime("%Y-%m-%d %H:%M:%S.000")

            sql = f'''SELECT [UserId],[bonus],[Level],[start_dd],[end_dd],[Modify_dd] FROM [dbo].[UserBonus]  WHERE UserId = '{self.UserId}' AND start_dd = '{start_dd}' '''
            cursor.execute(sql)
            result = cursor.fetchone()
            if result:
                ori_predict_num = int(result[1])
                predict_num += ori_predict_num

            if predict_num >=10 and predict_num<20:
                Level= '銅'
            elif predict_num >=20 and predict_num<30:
                Level = '銀'
            elif predict_num >=30 and predict_num<50:
                Level = '金'
            elif predict_num >=50 and predict_num<60:
                Level = '白金'
            elif predict_num >= 60 and predict_num<70:
                Level = '鑽石'
            elif predict_num >= 70:
                Level = '菁英'
            else:
                Level = '無'

            if result:
                update_sql = f'''UPDATE [dbo].[UserBonus] SET [bonus]='{float(predict_num):.2f}',[Level]=N'{Level}',[start_dd]='{start_dd}',[end_dd]='{end_dd}',[Modify_dd]='{Modify_dd}' WHERE UserId = '{self.UserId}' AND start_dd = '{start_dd}' '''
                print(datetime.now().strftime('%Y/%m/%d %H:%M:%S'), '-' * 10,'執行', update_sql)
                cursor.execute(update_sql)
                db.commit()
            else:
                insert_sql = f'''INSERT INTO [dbo].[UserBonus]([UserId],[bonus],[Level],[start_dd],[end_dd],[Modify_dd])VALUES('{self.UserId}','{float(predict_num):.2f}',N'{Level}','{start_dd}','{end_dd}','{Modify_dd}')'''
                print(datetime.now().strftime('%Y/%m/%d %H:%M:%S'), '-' * 10,'執行', insert_sql)
                cursor.execute(insert_sql)
                db.commit()
        else:
            print(datetime.now().strftime('%Y/%m/%d %H:%M:%S'), '-' * 10,'predict_num <= 0')

    def start(self):
        self.PredictMatchEntrys()

    def get_ConnectionFromDB(self):
        db = pymssql.connect(self.server,self.user,self.password,self.database)
        cursor = db.cursor()
        return db, cursor

    def FetchDateTimeRange(self, day=1):
        #return (datetime.now().replace(microsecond=0) - timedelta(days=0)).replace(hour=0, minute=0, second=0, microsecond=0), (datetime.now() + timedelta(days=1)).replace(hour=23, minute=59, second=59, microsecond=0)
        return (datetime.now().replace(microsecond=0)- timedelta(days=0)),(datetime.now() + timedelta(days=1)).replace(hour=23, minute=59,second=59,microsecond=0)

    def process_DataTime(self, Date, Start):
        return datetime.strptime(Date + ' ' + Start.spilt('(')[0].strip(), "%Y-%m-%d %H:%M")

    def getMatchEntrysByTimeRange(self, db, top, bottom, cursor):
        sql = f'''SELECT MatchEntry.EventCode,MatchTime,HomeTeam,AwayTeam,Odds.GroupOptionCode,Odds.OptionCode,Odds.OptionRate
                  from MatchEntry
                  inner join Odds on Odds.EventCode = MatchEntry.EventCode
                  where TournamentText = '{self.TournamentText}' and Odds.SourceCode='{self.SourceCode}'
                        and MatchTime >= '{top}' and MatchTime <= '{bottom}' order by MatchTime '''
        print(datetime.now().strftime('%Y/%m/%d %H:%M:%S'), '-' * 10,'執行',sql)
        cursor.execute(sql)
        return pd.read_sql(sql, db)

    def PredictMatchEntrys(self):
            pd.set_option('display.max_columns', None)
            predict_num = 0
            db, cursor = self.get_ConnectionFromDB()
            top , bottom = self.FetchDateTimeRange()
            # 整理開賽玩法
            print(datetime.now().strftime('%Y/%m/%d %H:%M:%S'), '-' * 10, f'整理開賽玩法')
            nba_df_new = {}
            Odds = self.getMatchEntrysByTimeRange(db, top, bottom, cursor)
            for GroupOptionCode in ['20']:
                nba_df_new[GroupOptionCode] = []
                Odds_for_GroupOptionCode = Odds[Odds.GroupOptionCode==GroupOptionCode]
                EventCodes = list(set(Odds_for_GroupOptionCode.EventCode))
                for EventCode in EventCodes:
                    Odds_for_EventCode = Odds_for_GroupOptionCode[Odds_for_GroupOptionCode.EventCode == EventCode]
                    nba_dfs = []
                    # 遍例主客場盤口
                    for _,odds_for_EventCode in Odds_for_EventCode.iterrows():
                        if odds_for_EventCode['OptionCode'] == '1':
                            nba_dfs.append(  dict(gmDate=odds_for_EventCode['MatchTime'].strftime('%Y/%m/%d'),
                                             gmTime=odds_for_EventCode['MatchTime'].strftime('%H:%M'),
                                             seasType='Regular',
                                             offLNm1='', offFNm1='', offLNm2='', offFNm2='', OffLNm3='', offFNm3='',
                                             teamAbbr=self.TeamNameCorrection(odds_for_EventCode['HomeTeam'],cursor=cursor,isTawian=0),teamConf='',teamLoc='Home',
                                             teamRslt='',teamMin='',teamDayOff='',
                                             teamPTS='',teamAST='',teamTO='',teamSTL='',teamBLK='',teamFGA='',teamFGM='',teamFG_Percentage='',team2PA='',team2PM='',
                                             team2P_Percentage='',team3PA='',team3PM='',team3P_Percentage='',teamFTA='',teamFTM='',teamFT_Percentage='',teamORB='',
                                             teamDRB='',teamTRB='',teamPTS1='',teamPTS2='',teamPTS3='',teamPTS4='',teamPTS5='',teamPTS6='',teamPTS7='', teamPTS8='',
                                             teamTREB_Percentage='',teamASST_Percentage='',teamTS_Percentage='',teamEFG_Percentage='',teamOREB_Percentage='',teamDREB_Percentage='',
                                             teamTO_Percentage='',teamSTL_Percentage='',teamBLK_Percentage='',teamBLKR='',teamPPS='',teamFIC='',teamFIC40='',
                                             teamOrtg='',teamDrtg='',teamEDiff='',teamPlay_Percentage='',teamAR='',teamASTDividedByTO='',teamSTLDividedByTO='',teamFouls='',

                                             opptAbbr=self.TeamNameCorrection(odds_for_EventCode['AwayTeam'],cursor=cursor,isTawian=0),opptConf='', opptLoc='Away',
                                             opptRslt='X',opptMin='',opptDayOff='',
                                             opptPTS='',opptAST='',opptTO='',opptSTL='',opptBLK='',opptFGA='',opptFGM='',opptFG_Percentage='',oppt2PA='',oppt2PM='',
                                             oppt2P_Percentage='',oppt3PA='',oppt3PM='',oppt3P_Percentage='',opptFTA='',opptFTM='',opptFT_Percentage='',opptORB='',
                                             opptDRB='',opptTRB='',opptPTS1='',opptPTS2='',opptPTS3='',opptPTS4='',opptPTS5='',opptPTS6='',opptPTS7='', opptPTS8='',
                                             opptTREB_Percentage='',opptASST_Percentage='',opptTS_Percentage='',opptEFG_Percentage='',opptOREB_Percentage='',opptDREB_Percentage='',
                                             opptTO_Percentage='',opptSTL_Percentage='',opptBLK_Percentage='',opptBLKR='',opptPPS='',opptFIC='',opptFIC40='',
                                             opptOrtg='',opptDrtg='',opptEDiff='',opptPlay_Percentage='',opptAR='',opptASTDividedByTO='',opptSTLDividedByTO='',opptFouls='',
                                             EventCode = odds_for_EventCode['EventCode'],DB_HomeOdds=odds_for_EventCode['OptionRate'])
                                             )
                        else:
                            nba_dfs.append(  dict(gmDate=odds_for_EventCode['MatchTime'].strftime('%Y/%m/%d'),
                                             gmTime=odds_for_EventCode['MatchTime'].strftime('%H:%M'),
                                             seasType='Regular',
                                             offLNm1='', offFNm1='', offLNm2='', offFNm2='', OffLNm3='', offFNm3='',
                                             teamAbbr=self.TeamNameCorrection(odds_for_EventCode['HomeTeam'],cursor=cursor,isTawian=0),teamConf='',teamLoc='Home',
                                             teamRslt='',teamMin='',teamDayOff='',
                                             teamPTS='',teamAST='',teamTO='',teamSTL='',teamBLK='',teamFGA='',teamFGM='',teamFG_Percentage='',team2PA='',team2PM='',
                                             team2P_Percentage='',team3PA='',team3PM='',team3P_Percentage='',teamFTA='',teamFTM='',teamFT_Percentage='',teamORB='',
                                             teamDRB='',teamTRB='',teamPTS1='',teamPTS2='',teamPTS3='',teamPTS4='',teamPTS5='',teamPTS6='',teamPTS7='', teamPTS8='',
                                             teamTREB_Percentage='',teamASST_Percentage='',teamTS_Percentage='',teamEFG_Percentage='',teamOREB_Percentage='',teamDREB_Percentage='',
                                             teamTO_Percentage='',teamSTL_Percentage='',teamBLK_Percentage='',teamBLKR='',teamPPS='',teamFIC='',teamFIC40='',
                                             teamOrtg='',teamDrtg='',teamEDiff='',teamPlay_Percentage='',teamAR='',teamASTDividedByTO='',teamSTLDividedByTO='',teamFouls='',

                                             opptAbbr=self.TeamNameCorrection(odds_for_EventCode['AwayTeam'],cursor=cursor,isTawian=0),opptConf='', opptLoc='Away',
                                             opptRslt='X',opptMin='',opptDayOff='',
                                             opptPTS='',opptAST='',opptTO='',opptSTL='',opptBLK='',opptFGA='',opptFGM='',opptFG_Percentage='',oppt2PA='',oppt2PM='',
                                             oppt2P_Percentage='',oppt3PA='',oppt3PM='',oppt3P_Percentage='',opptFTA='',opptFTM='',opptFT_Percentage='',opptORB='',
                                             opptDRB='',opptTRB='',opptPTS1='',opptPTS2='',opptPTS3='',opptPTS4='',opptPTS5='',opptPTS6='',opptPTS7='', opptPTS8='',
                                             opptTREB_Percentage='',opptASST_Percentage='',opptTS_Percentage='',opptEFG_Percentage='',opptOREB_Percentage='',opptDREB_Percentage='',
                                             opptTO_Percentage='',opptSTL_Percentage='',opptBLK_Percentage='',opptBLKR='',opptPPS='',opptFIC='',opptFIC40='',
                                             opptOrtg='',opptDrtg='',opptEDiff='',opptPlay_Percentage='',opptAR='',opptASTDividedByTO='',opptSTLDividedByTO='',opptFouls='',
                                             EventCode = odds_for_EventCode['EventCode'],DB_AwayOdds=odds_for_EventCode['OptionRate'])
                                             )
                    nba_df_new[GroupOptionCode].append(nba_dfs[0]|nba_dfs[1]) # 合併盤主客場盤口
            # 預測強弱盤
            if nba_df_new[GroupOptionCode] != []:
                predict_num += self.predict_StrongWeak(nba_df_new[GroupOptionCode],cursor=cursor,db=db)
                self.add_userbouns(predict_num,cursor=cursor,db=db)
            else:
                print(datetime.now().strftime('%Y/%m/%d %H:%M:%S'), '-' * 10, f'沒有開場賽事')
            cursor.close()
            db.close()

    def TeamNameCorrection(self, TeamName, cursor, isTawian=1):
        if isTawian:
            sql = f"SELECT teams.team FROM teamText join teams on teamText.team_id = teams.id where Text = '{TeamName}' ;"
            cursor.execute(sql)
            result = cursor.fetchone()
            if result:
                print(datetime.now().strftime('%Y/%m/%d %H:%M:%S'), '-' * 10,f'{TeamName}更換名稱為{result[0]}')
                return result[0]
            else:
                return ''
        else:
            sql = f"SELECT teamText.Text FROM teamText join teams on teamText.team_id = teams.id where teams.team = '{TeamName}' ;"
            cursor.execute(sql)
            result = cursor.fetchone()
            if result:
                print(datetime.now().strftime('%Y/%m/%d %H:%M:%S'), '-' * 10,f'{TeamName}更換名稱為{result[0]}')
                return result[0]
            else:
                return ''

    def predict_StrongWeak(self, nba_df_new,cursor,db):
        predict_num = 0

        # 合併開場賽事與賠率
        print(datetime.now().strftime('%Y/%m/%d %H:%M:%S'), '-' * 10, f'合併開場賽事與賠率')
        nba_odds_df = get_TeamGameOdds.get_MatchEntrysFromNotStarted()
        nba_odds_df['gmDateTime'] = nba_odds_df['gmDate'] + ' ' + nba_odds_df['gmTime']
        nba_odds_df['gmDateTime'] = pd.to_datetime(nba_odds_df['gmDateTime'])
        nba_df_new = pd.DataFrame(nba_df_new)
        nba_df_new = pd.concat([nba_df_new, pd.DataFrame(columns=['home1x2FirstOptionRate', 'away1x2FirstOptionRate', 'home1x2LastOptionRate','away1x2LastOptionRate',
                                                          'home1x2FirstWinRate','away1x2FirstWinRate','home1x2LastWinRate','away1x2LastWinRate',
                                                          'home1x2FirstRTP','away1x2LastRTP','home1x2KellyIndex','away1x2KellyIndex',
                                                          'OverFirstOptionRate','UnderFirstOptionRate','OverUnderFirstSpecialBetValue','OverLastOptionRate',
                                                          'UnderLastOptionRate','OverUnderLastSpecialBetValue','SourceUrl'])])
        nba_df_new = nba_df_new.replace('', np.nan, regex=True)
        for idx in nba_df_new.index.values.tolist():
            nba_df_MatchTime = datetime.strptime(nba_df_new.loc[idx,'gmDate'] + ' ' + nba_df_new.loc[idx,'gmTime'], "%Y/%m/%d %H:%M")
            offset_sec = 120 * 60
            top = datetime.fromtimestamp(time.mktime(nba_df_MatchTime.timetuple()) + offset_sec)
            bottom = datetime.fromtimestamp(time.mktime(nba_df_MatchTime.timetuple()) - offset_sec)
            #print(nba_df_new.loc[idx,'teamAbbr'],nba_df_new.loc[idx,'opptAbbr'],bottom,top)
            nba_odds_slice_df = nba_odds_df[((nba_df_new.loc[idx,'teamAbbr'] == nba_odds_df.homeAbbr) & (nba_df_new.loc[idx,'opptAbbr'] == nba_odds_df.awayAbbr) |
                                             (nba_df_new.loc[idx,'opptAbbr'] == nba_odds_df.homeAbbr) & (nba_df_new.loc[idx,'teamAbbr']== nba_odds_df.awayAbbr)) &
                                             (nba_odds_df['gmDateTime'] >= bottom) & (nba_odds_df['gmDateTime'] < top)][:1]

            for _, odds_slice in nba_odds_slice_df.iterrows():
                nba_df_new.loc[idx, 'home1x2FirstOptionRate'] = odds_slice.home1x2FirstOptionRate
                nba_df_new.loc[idx, 'away1x2FirstOptionRate'] = odds_slice.away1x2FirstOptionRate
                nba_df_new.loc[idx, 'home1x2LastOptionRate'] = odds_slice.home1x2LastOptionRate
                nba_df_new.loc[idx, 'away1x2LastOptionRate'] = odds_slice.away1x2LastOptionRate
                nba_df_new.loc[idx, 'home1x2FirstWinRate'] = odds_slice.home1x2FirstWinRate
                nba_df_new.loc[idx, 'away1x2FirstWinRate'] = odds_slice.away1x2FirstWinRate
                nba_df_new.loc[idx, 'home1x2LastWinRate'] = odds_slice.home1x2LastWinRate
                nba_df_new.loc[idx, 'away1x2LastWinRate'] = odds_slice.away1x2LastWinRate
                nba_df_new.loc[idx, 'home1x2FirstRTP'] = odds_slice.home1x2FirstRTP
                nba_df_new.loc[idx, 'away1x2LastRTP'] = odds_slice.away1x2LastRTP
                nba_df_new.loc[idx, 'home1x2KellyIndex'] = odds_slice.home1x2KellyIndex
                nba_df_new.loc[idx, 'away1x2KellyIndex'] = odds_slice.away1x2KellyIndex
                nba_df_new.loc[idx, 'OverFirstOptionRate'] = odds_slice.OverFirstOptionRate
                nba_df_new.loc[idx, 'UnderFirstOptionRate'] = odds_slice.UnderFirstOptionRate
                nba_df_new.loc[idx, 'OverUnderFirstSpecialBetValue'] = odds_slice.OverUnderFirstSpecialBetValue
                nba_df_new.loc[idx, 'OverLastOptionRate'] = odds_slice.OverLastOptionRate
                nba_df_new.loc[idx, 'UnderLastOptionRate'] = odds_slice.UnderLastOptionRate
                nba_df_new.loc[idx, 'OverUnderLastSpecialBetValue'] = odds_slice.OverUnderLastSpecialBetValue
                nba_df_new.loc[idx, 'SourceUrl'] = odds_slice.SourceUrl

            print(datetime.now().strftime('%Y/%m/%d %H:%M:%S'), '-' * 10, f'結合後 -- > {nba_df_new.loc[idx, :].to_dict()}')
        nba_df_new = nba_df_new.replace('', np.nan, regex=True)
        nba_df_new.to_csv('C:/預測機器人NBA(第二版)/nba_df_new.csv',index=None)
        nba_df_new = pd.read_csv('C:/預測機器人NBA(第二版)/nba_df_new.csv',dtype={'EventCode':str})
        # 資料前處理
        print(datetime.now().strftime('%Y/%m/%d %H:%M:%S'), '-' * 10, f'資料前處理')
        X,nba_df_for_ML = self.process_nba_df_data(nba_df_new)
        # print(datetime.now().strftime('%Y/%m/%d %H:%M:%S'), '-' * 10,nba_df_for_ML[~nba_df_for_ML.EventCode.isnull()])
        # 模型預測與寫入
        print(datetime.now().strftime('%Y/%m/%d %H:%M:%S'), '-' * 10, f'模型預測')
        GradientBoosting_best = joblib.load('C:/預測機器人NBA(第二版)/強弱盤_GB.model')
        # 取出超過門檻的預測值
        results = []
        for index in nba_df_for_ML[~nba_df_for_ML.EventCode.isnull()].index:
            result = GradientBoosting_best.predict_proba(np.array([X.loc[index]]))
            if result[0][1] > result[0][0] and result[0][1] >= 0.9:
                results.append([index, 1, result[0][1]])
            if result[0][1] < result[0][0] and result[0][0] >= 0.6:
                results.append([index, 0, result[0][0]])

        for result in results:
            game = nba_df_for_ML.loc[result[0]]
            PredictTeam = game.homeAbbr if result[1] == 1 else game.awayAbbr
            OptionCode = 1 if result[1] == 1 else 2
            SpecialBetValue = ''
            OptionRate = game.DB_HomeOdds if result[1] == 1 else game.DB_AwayOdds

            if not self.isPredictMacthExists(self.UserId,game.EventCode,self.GroupOptionCode,cursor=cursor,db=db):
                predict_num += 1
                for gameType in self.gameType:
                    insert_sql = f'''INSERT INTO [dbo].[PredictMatch] ([UserId],[SportCode],[EventType],[EventCode],[TournamentCode],[TournamentText],[GroupOptionCode],[GroupOptionName]
                                                      ,[PredictTeam],[OptionCode],[SpecialBetValue],[OptionRate],[status],[gameType],[MarketType],[PredictDatetime],[CreatedTime]) VALUES
                                                      ('{self.UserId}','{self.SportCode}', '{self.EventType}','{game.EventCode}', '{self.SportTournamentCode}','{self.TournamentText}','{self.GroupOptionCode}','{self.GroupOptionName}',
                                                       '{self.TeamNameCorrection(PredictTeam,cursor=cursor)}','{OptionCode}','{SpecialBetValue}','{OptionRate}','{self.status}','{gameType}','{self.MarketType}','{datetime.now().replace(microsecond=0).strftime("%Y-%m-%d %H:%M:%S.000")}'
                                                      ,'{datetime.now().replace(microsecond=0).strftime("%Y-%m-%d %H:%M:%S.000")}') '''
                    print(datetime.now().strftime('%Y/%m/%d %H:%M:%S'), '-' * 10,insert_sql)
                    cursor.execute(insert_sql)
                    db.commit()

            else:
                update_sql = f'''UPDATE [dbo].[PredictMatch] SET [PredictTeam]='{self.TeamNameCorrection(PredictTeam,cursor=cursor)}',[OptionCode]='{OptionCode}',[SpecialBetValue]='{SpecialBetValue}',[OptionRate]='{OptionRate}',[PredictDatetime]='{datetime.now().replace(microsecond=0).strftime("%Y-%m-%d %H:%M:%S.000")}',[CreatedTime]='{datetime.now().replace(microsecond=0).strftime("%Y-%m-%d %H:%M:%S.000")}'
                                 WHERE UserId = '{self.UserId}' and EventCode = '{game.EventCode}' and GroupOptionCode = '{self.GroupOptionCode}' '''
                print(datetime.now().strftime('%Y/%m/%d %H:%M:%S'), '-' * 10,update_sql)
                # cursor.execute(update_sql)
                # db.commit()

        return predict_num

    def isPredictMacthExists(self, UserId,EventCode,GroupOptionCode,cursor,db):
       sql = f'''SELECT * FROM [PredictMatch] where UserId = '{UserId}' and EventCode = '{EventCode}' and GroupOptionCode = '{GroupOptionCode}' '''
       print(datetime.now().strftime('%Y/%m/%d %H:%M:%S'), '-' * 10, sql)
       cursor.execute(sql)
       results = cursor.fetchall()
       if len(results)>0:
           return True
       else:
           return False

    def process_nba_df_data(self, nba_df_new):
        DateTime = datetime.now()
        year = DateTime.year - 1 if DateTime.month < 10 else DateTime.year
        nba_df = pd.read_csv(f"C:/python_projects_temp/NBA_Predict/NBA_DATA3-5/{year}_TeamGameStatsAndOdds.csv",dtype={'EventCode':str})
        nba_df = pd.concat([nba_df, nba_df_new], ignore_index=True)
        # 刪除資料欄
        cols_with_missing = ['offLNm1', 'offFNm1', 'offLNm2', 'offFNm2', 'OffLNm3', 'offFNm3',
                             'teamConf', 'teamDayOff', 'team2PA', 'team2PM', 'team2P_Percentage', 'teamPTS1', 'teamPTS2',
                             'teamPTS3', 'teamPTS4', 'teamPTS5', 'teamPTS6', 'teamPTS7', 'teamPTS8', 'teamBLKR',
                             'teamPPS', 'teamFIC', 'teamFIC40', 'teamEDiff', 'teamPlay_Percentage', 'teamAR',
                             'opptConf', 'opptDayOff', 'oppt2PA', 'oppt2PM', 'oppt2P_Percentage', 'opptPTS1', 'opptPTS2',
                             'opptPTS3', 'opptPTS4', 'opptPTS5', 'opptPTS6', 'opptPTS7', 'opptPTS8', 'opptBLKR',
                             'opptPPS', 'opptFIC', 'opptFIC40', 'opptEDiff', 'opptPlay_Percentage', 'opptAR']

        nba_df.drop(cols_with_missing, axis=1, inplace=True)
        # 刪除不相關的資料
        cols_with_irrelevant = ['seasType', 'teamMin', 'opptLoc', 'opptRslt', 'opptMin',
                                'OverFirstOptionRate', 'UnderFirstOptionRate', 'OverUnderFirstSpecialBetValue',
                                'OverLastOptionRate', 'UnderLastOptionRate', 'OverUnderLastSpecialBetValue', 'SourceUrl']
        nba_df.drop(cols_with_irrelevant, axis=1, inplace=True)
        # 離散資料轉連續數值
        nba_df['teamLoc'] = nba_df['teamLoc'].replace(['Home', 'Away'], [1, 0]).astype(str).astype(int)
        nba_df['teamRslt'] = nba_df['teamRslt'].replace(['Win', 'Loss', np.nan], [1, 0, -1]).astype(str).astype(int)
        # 因為一場比賽被劃分成兩筆，所以只取前面為主場那筆
        nba_df = nba_df[nba_df.teamLoc == 1].reset_index()
        del nba_df['index']
        del nba_df['teamLoc']
        # 更改欄位名稱
        nba_df.columns = nba_df.columns.str.replace('team', 'home')
        nba_df.columns = nba_df.columns.str.replace('oppt', 'away')
        nba_df.columns = nba_df.columns.str.replace('home1x2FirstRTP', '1x2FirstRTP')
        nba_df.columns = nba_df.columns.str.replace('away1x2LastRTP', '1x2LastRTP')

        # 補值
        def Complement(values):
            FirstValue = values[0]
            FinalValue = values[1]
            if pd.isnull(FinalValue):
                return FirstValue
            else:
                return FinalValue

        nba_df['home1x2LastOptionRate'] = nba_df[['home1x2FirstOptionRate', 'home1x2LastOptionRate']].apply(Complement,axis=1)
        nba_df['away1x2LastOptionRate'] = nba_df[['away1x2FirstOptionRate', 'away1x2LastOptionRate']].apply(Complement,axis=1)
        nba_df['home1x2LastWinRate'] = nba_df[['home1x2FirstWinRate', 'home1x2LastWinRate']].apply(Complement, axis=1)
        nba_df['away1x2LastWinRate'] = nba_df[['away1x2FirstWinRate', 'away1x2LastWinRate']].apply(Complement, axis=1)
        nba_df['1x2LastRTP'] = nba_df[['1x2FirstRTP', '1x2LastRTP']].apply(Complement, axis=1)
        # 刪除空資料列(沒有賠率的)
        delete_indexs = nba_df[nba_df.away1x2FirstOptionRate.isnull()].index
        nba_df = nba_df.drop(delete_indexs, axis=0).reset_index(drop=True)
        # Player efficiency rating 績效指數評級 (PIR) 是一個總體績效指標。
        homePIR = ((nba_df['homePTS'] + nba_df['homeTRB'] + nba_df['homeAST']
                    + nba_df['homeSTL'] + nba_df['homeBLK'] + nba_df['awayFouls'])
                   # Missed Field Goals:
                   - ((nba_df['homeFGA'] - nba_df['homeFGM'])
                      # Missed Free Throws:
                      + (nba_df['homeFTA'] - nba_df['homeFTM'])
                      + nba_df['homeTO'] + nba_df['awayBLK'] + nba_df['homeFouls']))

        awayPIR = ((nba_df['awayPTS'] + nba_df['awayTRB'] + nba_df['awayAST']
                    + nba_df['awaySTL'] + nba_df['awayBLK'] + nba_df['homeFouls'])
                   # Missed Field Goals:
                   - ((nba_df['awayFGA'] - nba_df['awayFGM'])
                      # Missed Free Throws:
                      + (nba_df['awayFTA'] - nba_df['awayFTM'])
                      + nba_df['awayTO'] + nba_df['homeBLK'] + nba_df['awayFouls']))
        nba_df['homePIR'] = pd.Series(homePIR)
        nba_df['awayPIR'] = pd.Series(awayPIR)
        # 新增其他特徵
        homePPS = nba_df.homePTS / nba_df.homeFGA
        homeFIC = nba_df.homePTS + nba_df.homeORB + 0.75 * nba_df.homeDRB + nba_df.homeAST + nba_df.homeSTL + nba_df.homeBLK - 0.75 * nba_df.homeFGA - 0.375 * nba_df.homeFTA - nba_df.homeTO - 0.5 * nba_df.homeFouls
        homeEDiff = nba_df.homeOrtg - nba_df.homeDrtg
        homePlay_Percentage = nba_df.homeFGM / (nba_df.homeFGA - nba_df.homeORB + nba_df.homeTO)
        homeAR = (nba_df.homeAST * 100) / (nba_df.homeFGA - 0.44 * nba_df.homeFTA + nba_df.homeAST + nba_df.homeTO)

        awayPPS = nba_df.awayPTS / nba_df.awayFGA
        awayFIC = nba_df.awayPTS + nba_df.awayORB + 0.75 * nba_df.awayDRB + nba_df.awayAST + nba_df.awaySTL + nba_df.awayBLK - 0.75 * nba_df.awayFGA - 0.375 * nba_df.awayFTA - nba_df.awayTO - 0.5 * nba_df.awayFouls
        awayEDiff = nba_df.awayOrtg - nba_df.awayDrtg
        awayPlay_Percentage = nba_df.awayFGM / (nba_df.awayFGA - nba_df.awayORB + nba_df.awayTO)
        awayAR = (nba_df.awayAST * 100) / (nba_df.awayFGA - 0.44 * nba_df.awayFTA + nba_df.awayAST + nba_df.awayTO)

        nba_df['homePPS'] = pd.Series(homePPS)
        nba_df['homeFIC'] = pd.Series(homeFIC)
        nba_df['homeEDiff'] = pd.Series(homeEDiff)
        nba_df['homePlay_Percentage'] = pd.Series(homePlay_Percentage)
        nba_df['homeAR'] = pd.Series(homeAR)

        nba_df['awayPPS'] = pd.Series(awayPPS)
        nba_df['awayFIC'] = pd.Series(awayFIC)
        nba_df['awayEDiff'] = pd.Series(awayEDiff)
        nba_df['awayPlay_Percentage'] = pd.Series(awayPlay_Percentage)
        nba_df['awayAR'] = pd.Series(awayAR)
        # 判斷是否為明星賽前後
        nba_df['gmDate'] = pd.to_datetime(nba_df['gmDate'], errors='coerce')
        nba_df['year'] = nba_df['gmDate'].dt.year
        nba_df['month'] = nba_df['gmDate'].dt.month

        def seasons(d):
            y = d[0]
            m = d[1]
            if (y == 2014 and m in (10, 11, 12)) or (y == 2015 and m in (1, 2, 3, 4)):
                s = 1
            elif (y == 2015 and m in (10, 11, 12)) or (y == 2016 and m in (1, 2, 3, 4)):
                s = 2
            elif (y == 2016 and m in (10, 11, 12)) or (y == 2017 and m in (1, 2, 3, 4)):
                s = 3
            elif (y == 2017 and m in (10, 11, 12)) or (y == 2018 and m in (1, 2, 3, 4)):
                s = 4
            elif (y == 2018 and m in (10, 11, 12)) or (y == 2019 and m in (1, 2, 3, 4)):
                s = 5
            elif (y == 2019 and m in (10, 11, 12)) or (y == 2020 and m in (1, 2, 3, 4)):
                s = 6
            elif (y == 2020 and m in (10, 11, 12)) or (y == 2021 and m in (1, 2, 3, 4)):
                s = 7
            elif (y == 2021 and m in (10, 11, 12)) or (y == 2022 and m in (1, 2, 3, 4)):
                s = 8
            else:
                s = 9
            return s
        nba_df['Season'] = nba_df[['year', 'month']].apply(seasons, axis=1)
        def halfs(x):
            if x in (10, 11, 12, 1):
                x = 'Pre_AllStar'
            else:
                x = 'Post_AllStar'
            return (x)
        nba_df['Season_half'] = nba_df['month'].apply(halfs)
        # 總得分差
        nba_df['homeDiffPTS'] = nba_df['homePTS'] - nba_df['awayPTS']
        nba_df['awayDiffPTS'] = nba_df['awayPTS'] - nba_df['homePTS']
        nba_df['absDiffPTS'] = abs(nba_df['homePTS'] - nba_df['awayPTS'])
        # 紀錄主客隊場次
        team_games = {}
        for index, row in nba_df[['homeAbbr', 'Season']].groupby(['homeAbbr', 'Season']).size().reset_index().sort_values(
                by=['homeAbbr', 'Season'])[:].iterrows():
            team_games[f"{row['homeAbbr']},{row['Season']}"] = 1
        home_games = []
        away_games = []
        for index, row in nba_df.iterrows():
            home_games.append(team_games[f"{row['homeAbbr']},{row['Season']}"])
            team_games[f"{row['homeAbbr']},{row['Season']}"] = team_games[f"{row['homeAbbr']},{row['Season']}"] + 1
            away_games.append(team_games[f"{row['awayAbbr']},{row['Season']}"])
            team_games[f"{row['awayAbbr']},{row['Season']}"] = team_games[f"{row['awayAbbr']},{row['Season']}"] + 1

        nba_df['home_games'] = pd.Series(home_games)
        nba_df['away_games'] = pd.Series(away_games)
        # DF for ML:
        nba_df_for_ML = nba_df[['homeRslt', 'gmDate', 'gmTime',
                                'homeAbbr', 'home_games', 'home1x2FirstOptionRate', 'home1x2LastOptionRate',
                                'home1x2FirstWinRate', 'home1x2LastWinRate', 'home1x2KellyIndex',
                                'awayAbbr', 'away_games', 'away1x2FirstOptionRate', 'away1x2LastOptionRate',
                                'away1x2FirstWinRate', 'away1x2LastWinRate', 'away1x2KellyIndex',
                                'Season_half', '1x2FirstRTP', '1x2LastRTP', 'EventCode', 'DB_HomeOdds',
                                'DB_AwayOdds']].copy()
        # 新增連勝特徵
        team_streak_dict = {}
        for index, row in nba_df[['homeAbbr', 'Season']].groupby(['homeAbbr', 'Season']).size().reset_index().sort_values(
                by=['homeAbbr', 'Season'])[:].iterrows():
            team_streak_dict[f"{row['homeAbbr']},{row['Season']}"] = 0
        home_streaks = []
        away_streaks = []
        for index, row in nba_df.iterrows():
            home_streaks.append(team_streak_dict[f"{row['homeAbbr']},{row['Season']}"])
            away_streaks.append(team_streak_dict[f"{row['awayAbbr']},{row['Season']}"])
            if row['homeRslt'] == 1:
                team_streak_dict[f"{row['homeAbbr']},{row['Season']}"] = team_streak_dict[f"{row['homeAbbr']},{row['Season']}"] + 1
                team_streak_dict[f"{row['awayAbbr']},{row['Season']}"] = 0
            else:
                team_streak_dict[f"{row['homeAbbr']},{row['Season']}"] = 0
                team_streak_dict[f"{row['awayAbbr']},{row['Season']}"] = team_streak_dict[f"{row['awayAbbr']},{row['Season']}"] + 1

        nba_df_for_ML['home_streak'] = pd.Series(home_streaks)
        nba_df_for_ML['away_streak'] = pd.Series(away_streaks)

        # 新增前5場平均表現特徵
        team_FiveLastGamesAvgDict = {}
        for index, row in nba_df[['homeAbbr', 'Season']].groupby(['homeAbbr', 'Season']).size().reset_index().sort_values(
                by=['homeAbbr', 'Season'])[:].iterrows():
            team_FiveLastGamesAvgDict[f"{row['homeAbbr']},{row['Season']}"] = nba_df[
                ((nba_df.homeAbbr == row['homeAbbr']) | (nba_df.awayAbbr == row['homeAbbr'])) & (
                            nba_df.Season == row['Season'])]
            team_FiveLastGamesAvgDict[f"{row['homeAbbr']},{row['Season']}"].reset_index(drop=True, inplace=True)
            team_FiveLastGamesAvgDict[f"{row['homeAbbr']},{row['Season']}"]['games'] = pd.Series(
                list(range(len(team_FiveLastGamesAvgDict[f"{row['homeAbbr']},{row['Season']}"])))) + 1

        home_cols = ['homePTS', 'homeAST', 'homeTO', 'homeSTL', 'homeBLK', 'homeFGA', 'homeFGM',
                     'homeFG_Percentage', 'home3PA', 'home3PM', 'home3P_Percentage',
                     'homeFTA', 'homeFTM', 'homeFT_Percentage', 'homeORB', 'homeDRB',
                     'homeTRB', 'homeTREB_Percentage', 'homeASST_Percentage',
                     'homeTS_Percentage', 'homeEFG_Percentage', 'homeOREB_Percentage',
                     'homeDREB_Percentage', 'homeTO_Percentage', 'homeSTL_Percentage',
                     'homeBLK_Percentage', 'homeOrtg', 'homeDrtg', 'homeASTDividedByTO',
                     'homeSTLDividedByTO', 'homeFouls', 'homePIR', 'homePPS', 'homeFIC',
                     'homeEDiff', 'homePlay_Percentage', 'homeAR', 'homeDiffPTS']

        away_cols = ['awayPTS', 'awayAST', 'awayTO', 'awaySTL', 'awayBLK', 'awayFGA', 'awayFGM',
                     'awayFG_Percentage', 'away3PA', 'away3PM', 'away3P_Percentage',
                     'awayFTA', 'awayFTM', 'awayFT_Percentage', 'awayORB', 'awayDRB',
                     'awayTRB', 'awayTREB_Percentage', 'awayASST_Percentage',
                     'awayTS_Percentage', 'awayEFG_Percentage', 'awayOREB_Percentage',
                     'awayDREB_Percentage', 'awayTO_Percentage', 'awaySTL_Percentage',
                     'awayBLK_Percentage', 'awayOrtg', 'awayDrtg', 'awayASTDividedByTO',
                     'awaySTLDividedByTO', 'awayFouls', 'awayPIR', 'awayPPS', 'awayFIC',
                     'awayEDiff', 'awayPlay_Percentage', 'awayAR', 'awayDiffPTS']

        # 第六場含以上 第一場-第五場平均 , 第五場 第一場-第四場平均 , 第四場 第一場-第三場平均 ... , 第一場 無
        def five_last_games_avg(home_col, away_col):
            home_FiveLastGamesAvgDict = {}
            away_FiveLastGamesAvgDict = {}
            for index, row in nba_df.iterrows():
                if row['home_games'] > 5:
                    home_games = row['home_games']
                    sum = 0
                    for idx5, row5 in team_FiveLastGamesAvgDict[f"{row['homeAbbr']},{row['Season']}"].query(
                            f'games>={home_games - 5} & games<{home_games}').iterrows():
                        if row['homeAbbr'] == row5['homeAbbr']:
                            sum += row5[home_col]
                        elif row['homeAbbr'] == row5['awayAbbr']:
                            sum += row5[away_col]
                    home_FiveLastGamesAvgDict.update({index: sum / 5})
                elif row['home_games'] <= 5 and row['home_games'] > 1:
                    home_games = row['home_games']
                    sum = 0
                    for idxX, rowX in team_FiveLastGamesAvgDict[f"{row['homeAbbr']},{row['Season']}"].query(
                            f'games>={home_games - (home_games - 1)} & games<{home_games}').iterrows():
                        if row['homeAbbr'] == rowX['homeAbbr']:
                            sum += rowX[home_col]
                        elif row['homeAbbr'] == rowX['awayAbbr']:
                            sum += rowX[away_col]
                    home_FiveLastGamesAvgDict.update({index: sum / (home_games - 1)})
                else:
                    home_FiveLastGamesAvgDict.update({index: 0})

                if row['away_games'] > 5:
                    away_games = row['away_games']
                    sum = 0
                    for idx5, row5 in team_FiveLastGamesAvgDict[f"{row['awayAbbr']},{row['Season']}"].query(
                            f'games>={away_games - 5} & games<{away_games}').iterrows():
                        if row['awayAbbr'] == row5['homeAbbr']:
                            sum += row5[home_col]
                        elif row['awayAbbr'] == row5['awayAbbr']:
                            sum += row5[away_col]
                    away_FiveLastGamesAvgDict.update({index: sum / 5})
                elif row['away_games'] <= 5 and row['away_games'] > 1:
                    away_games = row['away_games']
                    sum = 0
                    for idxX, rowX in team_FiveLastGamesAvgDict[f"{row['awayAbbr']},{row['Season']}"].query(
                            f'games>={away_games - (away_games - 1)} & games<{away_games}').iterrows():
                        if row['awayAbbr'] == rowX['homeAbbr']:
                            sum += rowX[home_col]
                        elif row['awayAbbr'] == rowX['awayAbbr']:
                            sum += rowX[away_col]
                    away_FiveLastGamesAvgDict.update({index: sum / (away_games - 1)})
                else:
                    away_FiveLastGamesAvgDict.update({index: 0})
            nba_df_for_ML[f'avg_{home_col}'] = pd.Series(home_FiveLastGamesAvgDict)
            nba_df_for_ML[f'avg_{away_col}'] = pd.Series(away_FiveLastGamesAvgDict)

        for home_col, away_col in zip(home_cols, away_cols):
            print(datetime.now().strftime('%Y/%m/%d %H:%M:%S'), '-' * 10,home_col,away_col,'已經處理完成.')
            five_last_games_avg(home_col, away_col)
        # 刪除nba_df_for_ML觀察用特徵:
        nba_df_for_ML_dumm = nba_df_for_ML.drop(
            ['gmDate', 'gmTime', 'homeAbbr', 'home_games', 'awayAbbr', 'away_games', 'EventCode', 'DB_HomeOdds',
             'DB_AwayOdds'], axis=1)

        # 虛擬變數
        nba_df_for_ML_dumm = pd.get_dummies(nba_df_for_ML_dumm)
        print(nba_df_for_ML_dumm.columns)
        # 正規化
        from sklearn.preprocessing import MinMaxScaler
        X = nba_df_for_ML_dumm.drop('homeRslt', axis=1)
        y = nba_df_for_ML_dumm.homeRslt
        scaler = MinMaxScaler(feature_range=(0, 1)).fit(X)
        X = pd.DataFrame(scaler.transform(X), index=X.index, columns=X.columns)
        return X,nba_df_for_ML

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('-p',
                        "--isPredict",
                        nargs=1,
                        type=int)
    parser.add_argument('-u',
                        "--isUpdate",
                        nargs=1,
                        type=int)
    args = parser.parse_args()
    NBAPredicter = NBAPredict()

    try:
        if args.isPredict[0]:
          NBAPredicter.start()
        elif args.isUpdate[0]:
          NBAPredicter.update_dataset()
    except:
        traceback.print_exc()

    print('end... 15秒後結束')
    time.sleep(15)






