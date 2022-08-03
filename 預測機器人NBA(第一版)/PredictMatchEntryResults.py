import pickle
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
import web_config

class NBAPredict(object):
    def __init__(self):
        self.SourceCode = 'Taiwan' # 台灣運彩編碼
        self.SportCode = 2 # NBA運動編碼: 2 來自SportCode table
        self.SportTournamentCode = '10041830' # 沿用
        self.EventType = 0
        self.CollectClient = 'NBA'
        self.server = web_config.production().server
        self.database = web_config.production().database 
        self.user = web_config.production().username
        self.password = web_config.production().password
        self.UserId = '6a7ac1ac-7b23-4c45-8e93-de1cce9d40f0' # koer3741@gmail.com
        self.TournamentText='NBA'
        self.status = 2
        self.gameType = ['Forecast','Selling']
        self.MarketType = 'international'

        with open("C:/預測機器人/強弱盤.pickle", 'rb') as f:
            self.StrongWeakModel = pickle.load(f)

        with open('C:/預測機器人/大小盤.pickle', 'rb') as f:
            self.BigSmallModel = pickle.load(f)

        with open('C:/預測機器人/讓分盤.pickle', 'rb') as f:
            self.Handicap = pickle.load(f)

    def start(self):
        self.initialize()
        self.PredictMatchEntrys()

    def get_ConnectionFromDB(self):
        db = pymssql.connect(self.server,self.user,self.password,self.database)
        cursor = db.cursor()
        return db, cursor

    def FetchDateTimeRange(self, day=1):
        return (datetime.now() + timedelta(days=1)).replace(hour=0, minute=0,second=0,microsecond=0) ,(datetime.now() + timedelta(days=1)).replace(hour=23, minute=59,second=59,microsecond=0)

    def process_DataTime(self, Date, Start):
        return datetime.strptime(Date + ' ' + Start[:-1]+'PM', "%a %b %d %Y %I:%M%p")

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
                print('執行', update_sql)
                cursor.execute(update_sql)
                db.commit()
            else:
                insert_sql = f'''INSERT INTO [dbo].[UserBonus]([UserId],[bonus],[Level],[start_dd],[end_dd],[Modify_dd])VALUES('{self.UserId}','{float(predict_num):.2f}',N'{Level}','{start_dd}','{end_dd}','{Modify_dd}')'''
                print('執行', insert_sql)
                cursor.execute(insert_sql)
                db.commit()


    def PredictMatchEntrys(self):
            predict_num = 0
            db, cursor = self.get_ConnectionFromDB()
            games = pd.read_csv('C:/預測機器人/NBA_DATA/明日賽事.csv')
            top , bottom = self.FetchDateTimeRange()
            for index, game in games.iterrows():
                try:
                    if self.process_DataTime(game['Date'],game['Start'])+ timedelta(hours=13) > top and self.process_DataTime(game['Date'],game['Start'])+ timedelta(hours=13) < bottom:
                        HomeTeam = self.TeamNameCorrection(game['Home/Neutral'],cursor)
                        AwayTeam = self.TeamNameCorrection(game['Visitor/Neutral'],cursor)
                        mydatatime = self.process_DataTime(game['Date'],game['Start'])+timedelta(hours=13)
                        Odds = self.get_Odds(mydatatime, 30, HomeTeam, AwayTeam, cursor)
                        if not Odds == []:
                            predict_num += len(Odds)/2
                            self.add_OddsToPredictMatch(Odds,cursor,db,game['Home/Neutral'], game['Visitor/Neutral'])
                        else:
                            print('沒開玩法')
                except:
                    print(traceback.format_exc())

            self.add_userbouns(predict_num,cursor,db)
            cursor.close()
            db.close()

    def add_OddsToPredictMatch(self, Odds, cursor, db, oriHome,oriVisitor):
        GroupOption = {'20': 'Moneyline', '51': 'Handicap', '52': 'Over/Under'}
        GroupOptionCount = {20: 0, 51: 0, 52: 0}

        PredictTeam, index = self.predict_StrongWeak(Odds[0][-2], Odds[0][-1], oriHome, oriVisitor)
        HomeTeamScore, AwayTeamScore = self.predict_Handicap(Odds[0][-2], Odds[0][-1], oriHome, oriVisitor)
        SUM = self.predict_SUM(Odds[0][-2], Odds[0][-1], oriHome, oriVisitor)

        for odds in Odds:

            GroupOptionCode, OptionCode, SpecialBetValue, OptionRate, EventCode, HomeTeam, AwayTeam = odds
            print(GroupOptionCode)


            if GroupOptionCode == '20' and GroupOptionCount[20] == 0 and OptionCode == str(index):
                GroupOptionCount[20] = 1
                for gameType in  self.gameType:
                    insert_sql = f'''INSERT INTO [dbo].[PredictMatch] ([UserId],[SportCode],[EventType],[EventCode],[TournamentCode],[TournamentText],[GroupOptionCode],[GroupOptionName]
                                 ,[PredictTeam],[OptionCode],[SpecialBetValue],[OptionRate],[status],[gameType],[MarketType],[PredictDatetime],[CreatedTime]) VALUES
                                 ('{self.UserId}','{self.SportCode}', '{self.EventType}','{EventCode}', '{self.SportTournamentCode}','{self.TournamentText}','{GroupOptionCode}','{GroupOption[GroupOptionCode]}',
                                  '{PredictTeam}','{index}','{''}','{OptionRate}','{self.status}','{gameType}','{self.MarketType}','{datetime.now().replace(microsecond=0).strftime("%Y-%m-%d %H:%M:%S.000")}'
                                  ,'{datetime.now().replace(microsecond=0).strftime("%Y-%m-%d %H:%M:%S.000")}') '''
                    print('執行', insert_sql)
                    cursor.execute(insert_sql)
                    db.commit()


            elif GroupOptionCode == '51' and GroupOptionCount[51] == 0:

                if abs(float(HomeTeamScore - AwayTeamScore)) < abs(float(SpecialBetValue)) and GroupOptionCount[51]==0 and int(OptionCode) == 1 and float(SpecialBetValue) > 0:
                    GroupOptionCount[51] = 1
                    for gameType in self.gameType:
                        insert_sql = f'''INSERT INTO [dbo].[PredictMatch] ([UserId],[SportCode],[EventType],[EventCode],[TournamentCode],[TournamentText],[GroupOptionCode],[GroupOptionName]
                                         ,[PredictTeam],[OptionCode],[SpecialBetValue],[OptionRate],[status],[gameType],[MarketType],[PredictDatetime],[CreatedTime]) VALUES
                                         ('{self.UserId}','{self.SportCode}', '{self.EventType}','{EventCode}', '{self.SportTournamentCode}','{self.TournamentText}','{GroupOptionCode}','{GroupOption[GroupOptionCode]}',
                                          '{[HomeTeam, AwayTeam][int(OptionCode)-1]}','{OptionCode}','{SpecialBetValue}','{OptionRate}','{self.status}','{gameType}','{self.MarketType}','{datetime.now().replace(microsecond=0).strftime("%Y-%m-%d %H:%M:%S.000")}'
                                          ,'{datetime.now().replace(microsecond=0).strftime("%Y-%m-%d %H:%M:%S.000")}') '''
                        print('執行', insert_sql)
                        cursor.execute(insert_sql)
                        db.commit()


                elif abs(float(HomeTeamScore - AwayTeamScore)) < abs(float(SpecialBetValue)) and GroupOptionCount[51]==0 and int(OptionCode) == 2 and float(SpecialBetValue) < 0:
                    GroupOptionCount[51] = 1
                    for gameType in self.gameType:
                        insert_sql = f'''INSERT INTO [dbo].[PredictMatch] ([UserId],[SportCode],[EventType],[EventCode],[TournamentCode],[TournamentText],[GroupOptionCode],[GroupOptionName]
                                         ,[PredictTeam],[OptionCode],[SpecialBetValue],[OptionRate],[status],[gameType],[MarketType],[PredictDatetime],[CreatedTime]) VALUES
                                         ('{self.UserId}','{self.SportCode}', '{self.EventType}','{EventCode}', '{self.SportTournamentCode}','{self.TournamentText}','{GroupOptionCode}','{GroupOption[GroupOptionCode]}',
                                          '{[HomeTeam, AwayTeam][int(OptionCode)-1]}','{OptionCode}','{SpecialBetValue}','{OptionRate}','{self.status}','{gameType}','{self.MarketType}','{datetime.now().replace(microsecond=0).strftime("%Y-%m-%d %H:%M:%S.000")}'
                                          ,'{datetime.now().replace(microsecond=0).strftime("%Y-%m-%d %H:%M:%S.000")}') '''
                        print('執行', insert_sql)
                        cursor.execute(insert_sql)
                        db.commit()


                elif abs(HomeTeamScore - AwayTeamScore) > abs(float(SpecialBetValue)) :
                    if HomeTeamScore > AwayTeamScore and GroupOptionCount[51]==0 and int(OptionCode)==1:
                        GroupOptionCount[51] = 1
                        for gameType in self.gameType:
                            insert_sql = f'''INSERT INTO [dbo].[PredictMatch] ([UserId],[SportCode],[EventType],[EventCode],[TournamentCode],[TournamentText],[GroupOptionCode],[GroupOptionName]
                                              ,[PredictTeam],[OptionCode],[SpecialBetValue],[OptionRate],[status],[gameType],[MarketType],[PredictDatetime],[CreatedTime]) VALUES
                                              ('{self.UserId}','{self.SportCode}', '{self.EventType}','{EventCode}', '{self.SportTournamentCode}','{self.TournamentText}','{GroupOptionCode}','{GroupOption[GroupOptionCode]}',
                                               '{HomeTeam}','{OptionCode}','{SpecialBetValue}','{OptionRate}','{self.status}','{gameType}','{self.MarketType}','{datetime.now().replace(microsecond=0).strftime("%Y-%m-%d %H:%M:%S.000")}'
                                               ,'{datetime.now().replace(microsecond=0).strftime("%Y-%m-%d %H:%M:%S.000")}') '''
                            print('執行', insert_sql)
                            cursor.execute(insert_sql)
                            db.commit()


                    elif HomeTeamScore < AwayTeamScore and GroupOptionCount[51]==0 and int(OptionCode) == 2:
                        GroupOptionCount[51] = 1
                        for gameType in self.gameType:
                            insert_sql = f'''INSERT INTO [dbo].[PredictMatch] ([UserId],[SportCode],[EventType],[EventCode],[TournamentCode],[TournamentText],[GroupOptionCode],[GroupOptionName]
                                              ,[PredictTeam],[OptionCode],[SpecialBetValue],[OptionRate],[status],[gameType],[MarketType],[PredictDatetime],[CreatedTime]) VALUES
                                              ('{self.UserId}','{self.SportCode}', '{self.EventType}','{EventCode}', '{self.SportTournamentCode}','{self.TournamentText}','{GroupOptionCode}','{GroupOption[GroupOptionCode]}',
                                               '{AwayTeam}','{OptionCode}','{SpecialBetValue}','{OptionRate}','{self.status}','{gameType}','{self.MarketType}','{datetime.now().replace(microsecond=0).strftime("%Y-%m-%d %H:%M:%S.000")}'
                                               ,'{datetime.now().replace(microsecond=0).strftime("%Y-%m-%d %H:%M:%S.000")}') '''
                            print('執行', insert_sql)
                            cursor.execute(insert_sql)
                            db.commit()


            elif odds[0] == '52' and GroupOptionCount[52] == 0 :
                if SUM > float(SpecialBetValue) and OptionCode == 'Over':
                    GroupOptionCount[52] = 1
                    for gameType in self.gameType:
                        insert_sql = f'''INSERT INTO [dbo].[PredictMatch] ([UserId],[SportCode],[EventType],[EventCode],[TournamentCode],[TournamentText],[GroupOptionCode],[GroupOptionName]
                                          ,[PredictTeam],[OptionCode],[SpecialBetValue],[OptionRate],[status],[gameType],[MarketType],[PredictDatetime],[CreatedTime]) VALUES
                                          ('{self.UserId}','{self.SportCode}', '{self.EventType}','{EventCode}', '{self.SportTournamentCode}','{self.TournamentText}','{GroupOptionCode}','{GroupOption[GroupOptionCode]}',
                                           '{''}','{OptionCode}','{SpecialBetValue}','{OptionRate}','{self.status}','{gameType}','{self.MarketType}','{datetime.now().replace(microsecond=0).strftime("%Y-%m-%d %H:%M:%S.000")}'
                                           ,'{datetime.now().replace(microsecond=0).strftime("%Y-%m-%d %H:%M:%S.000")}') '''
                        print('執行', insert_sql)
                        cursor.execute(insert_sql)
                        db.commit()

                elif SUM < float(SpecialBetValue) and OptionCode == 'Under':
                    GroupOptionCount[52] = 1
                    for gameType in self.gameType:
                        insert_sql = f'''INSERT INTO [dbo].[PredictMatch] ([UserId],[SportCode],[EventType],[EventCode],[TournamentCode],[TournamentText],[GroupOptionCode],[GroupOptionName]
                                          ,[PredictTeam],[OptionCode],[SpecialBetValue],[OptionRate],[status],[gameType],[MarketType],[PredictDatetime],[CreatedTime]) VALUES
                                          ('{self.UserId}','{self.SportCode}', '{self.EventType}','{EventCode}', '{self.SportTournamentCode}','{self.TournamentText}','{GroupOptionCode}','{GroupOption[GroupOptionCode]}',
                                           '{''}','{OptionCode}','{SpecialBetValue}','{OptionRate}','{self.status}','{gameType}','{self.MarketType}','{datetime.now().replace(microsecond=0).strftime("%Y-%m-%d %H:%M:%S.000")}'
                                           ,'{datetime.now().replace(microsecond=0).strftime("%Y-%m-%d %H:%M:%S.000")}') '''
                        print('執行', insert_sql)
                        cursor.execute(insert_sql)
                        db.commit()



    def get_Odds(self, mydatatime, offset_minute, HomeTeam, AwayTeam, cursor):
        offset_sec = offset_minute*60
        #d = datetime.strptime(mydatatime, "%Y-%m-%d %H:%M:%S.000")
        timestamp = time.mktime(mydatatime.timetuple())
        top = datetime.fromtimestamp(timestamp+offset_sec).strftime("%Y-%m-%d %H:%M:%S")
        bottom = datetime.fromtimestamp(timestamp-offset_sec).strftime("%Y-%m-%d %H:%M:%S")

        sql = f'''SELECT  GroupOptionCode,OptionCode,SpecialBetValue,OptionRate,MatchEntry.EventCode,HomeTeam,AwayTeam FROM Odds join MatchEntry on MatchEntry.EventCode = Odds.EventCode 
                  where MatchEntry.SourceCode = 'Bet365' AND MatchTime > '{bottom}' AND MatchTime < '{top}' AND HomeTeam = '{HomeTeam}' AND AwayTeam = '{AwayTeam}' '''
        print('執行',sql)
        cursor.execute(sql)

        result = cursor.fetchall()
        if result:
            return result
        else:
            return []


    def TeamNameCorrection(self,Taiwan_TeamName, cursor):
        sql = f"SELECT teams.team FROM teamText join teams on teamText.team_id = teams.id where Text = '{Taiwan_TeamName}' ;"
        cursor.execute(sql)
        result = cursor.fetchone()
        if result:
            print(f'{Taiwan_TeamName}更換名稱為{result[0]}')
            return result[0]
        else:
            return Taiwan_TeamName

    def predict_Handicap(self, home, visitor, oriHome, oriVisitor):
        features = []

        features.append(self.get_elo(oriHome) + 100)
        for key, value in self.team_stats.loc[oriHome].iteritems():
            features.append(value)

        features.append(self.get_elo(oriVisitor))
        for key, value in self.team_stats.loc[oriVisitor].iteritems():
            features.append(value)

        features = np.nan_to_num(features)
        HomeTeamScore, AwayTeamScore = self.Handicap.predict([features])[0]

        return HomeTeamScore, AwayTeamScore

    def predict_SUM(self, home, visitor, oriHome, oriVisitor):
        features = []

        features.append(self.get_elo(oriHome) + 100)
        for key, value in self.team_stats.loc[oriHome].iteritems():
            features.append(value)

        features.append(self.get_elo(oriVisitor))
        for key, value in self.team_stats.loc[oriVisitor].iteritems():
            features.append(value)

        features = np.nan_to_num(features)
        return self.BigSmallModel.predict([features])[0]

    def predict_StrongWeak(self, home, visitor, oriHome, oriVisitor):
        features = []
        teams = [home, visitor]

        features.append(self.get_elo(oriHome) + 100)
        for key, value in self.team_stats.loc[oriHome].iteritems():
            features.append(value)

        features.append(self.get_elo(oriVisitor))
        for key, value in self.team_stats.loc[oriVisitor].iteritems():
            features.append(value)

        features = np.nan_to_num(features)
        index = self.StrongWeakModel.predict([features])[0]
        return teams[index],index+1

    # 計算ELO值，分辨隊伍強隊
    def get_elo(self,team):
        try:
            return self.team_elos[team]
        except:
            self.team_elos[team] = self.base_elo
            return self.team_elos[team]

    def calc_elo(self,win_team, lose_team):
        winner_rank = self.get_elo(win_team)
        loser_rank = self.get_elo(lose_team)

        rank_diff = winner_rank - loser_rank
        exp = (rank_diff * -1) / 400
        odds = 1 / (1 + math.pow(10, exp))
        if winner_rank < 2100:
            k = 32
        elif winner_rank >= 2100 and winner_rank < 2400:
            k = 24
        else:
            k = 16
        new_winner_rank = round(winner_rank + (k * (1 - odds)))
        new_rank_diff = new_winner_rank - winner_rank
        new_loser_rank = loser_rank - new_rank_diff

        return new_winner_rank, new_loser_rank

    def initialize(self):
        path = 'C:/預測機器人/NBA_DATA'
        seasons = sorted(listdir(path))[0:-2]
        Mstat = pd.DataFrame()
        for season in seasons:
            Mstat = pd.concat([Mstat, pd.read_csv(os.path.join(path, season, 'Miscellaneous_Stat.csv'))],
                              ignore_index=True)

        def Team_map(team):
            return team if team.find('*') == -1 else team[:-1]


        Mstat.Team = Mstat.Team.map(Team_map)
        Mstat = Mstat.groupby('Team').mean().reset_index()

        path = 'C:/預測機器人/NBA_DATA'
        seasons = sorted(listdir(path))[0:-2]
        Tstat = pd.DataFrame()
        for season in seasons:
            Tstat = pd.concat([Tstat, pd.read_csv(os.path.join(path, season, 'Team_Per_Game_Stat.csv'))],
                              ignore_index=True)

        def Team_map(team):
            return team if team.find('*') == -1 else team[:-1]

        Tstat.Team = Tstat.Team.map(Team_map)
        Tstat = Tstat.groupby('Team').mean().reset_index()

        path = 'C:/預測機器人/NBA_DATA'
        seasons = sorted(listdir(path))[0:-2]
        Ostat = pd.DataFrame()
        for season in seasons:
            Ostat = pd.concat([Ostat, pd.read_csv(os.path.join(path, season, 'Opponent_Per_Game_Stat.csv'))],
                              ignore_index=True)

        def Team_map(team):
            return team if team.find('*') == -1 else team[:-1]

        Ostat.Team = Ostat.Team.map(Team_map)
        Ostat = Ostat.groupby('Team').mean().reset_index()

        def initialize_data(Mstat, Ostat, Tstat):
            new_Mstat = Mstat.drop(['Rk'], axis=1)
            new_Ostat = Ostat.drop(['Rk', 'G', 'MP'], axis=1)
            new_Tstat = Tstat.drop(['Rk', 'G', 'MP'], axis=1)

            team_stats1 = pd.merge(new_Mstat, new_Ostat, how='left', on='Team')
            team_stats1 = pd.merge(team_stats1, new_Tstat, how='left', on='Team')
            return team_stats1.set_index('Team', inplace=False, drop=True)

        self.team_stats = initialize_data(Mstat, Ostat, Tstat)


        path = 'C:/預測機器人/NBA_DATA'
        seasons = sorted(listdir(path))[0:-1]
        Games = pd.DataFrame()
        for season in seasons:
            Games = pd.concat([Games, pd.read_csv(os.path.join(path, season, 'result.csv'))], ignore_index=True)

        H_score = []
        A_score = []
        SUM = []
        for index, row in Games[['Visitor/Neutral', 'PTS', 'Home/Neutral', 'PTS.1']].iterrows():
            H_score.append(row['PTS.1'])
            A_score.append(row['PTS'])
            SUM.append(row['PTS.1'] + row['PTS'])

        Games["H_score"] = H_score
        Games["A_score"] = A_score
        Games["SUM"] = SUM

        FTR = []
        for index, row in Games[['Visitor/Neutral', 'PTS', 'Home/Neutral', 'PTS.1']].iterrows():
            if row['PTS'] > row['PTS.1']:
                FTR.append('Visitor')
            else:
                FTR.append('Home')

        Games["FTR"] = FTR
        Games = Games[['Visitor/Neutral', 'Home/Neutral', 'A_score', 'H_score', 'SUM', "FTR"]]

        import random, math

        self.base_elo = 1600
        self.team_elos = {}
        x = []
        y = []

        def build_dataSet(all_data):

            print("Building data set..")
            skip = 0
            for index, row in all_data.iterrows():

                Visitor = row['Visitor/Neutral']
                Home = row['Home/Neutral']
                H_score = row['H_score']
                A_score = row['A_score']

                Home_elo = self.get_elo(Home)
                Visitor_elo = self.get_elo(Visitor)

                # 给主场比赛的队伍加上100的elo值
                Home_elo += 100

                Home_features = [Home_elo]
                Visitor_features = [Visitor_elo]

                for key, value in self.team_stats.loc[Home].iteritems():
                    Home_features.append(value)
                for key, value in self.team_stats.loc[Visitor].iteritems():
                    Visitor_features.append(value)

                x.append(Home_features + Visitor_features)

                y.append([H_score, A_score])

                if row['FTR'] == 'Home':
                    new_winner_rank, new_loser_rank = self.calc_elo(Home, Visitor)
                    self.team_elos[Home] = new_winner_rank
                    self.team_elos[Visitor] = new_loser_rank
                else:
                    new_winner_rank, new_loser_rank = self.calc_elo(Visitor, Home)
                    self.team_elos[Visitor] = new_winner_rank
                    self.team_elos[Home] = new_loser_rank
            return np.nan_to_num(x), y

        build_dataSet(Games)

NBAPredicter = NBAPredict()

NBAPredicter.start()

'''a.initialize()
print(a.predict_StrongWeak('Orlando Magic','New Orleans Pelicans'))
print(a.predict_SUM('Orlando Magic','New Orleans Pelicans'))
print(a.predict_Handicap('Orlando Magic','New Orleans Pelicans'))

print((datetime.now().astimezone(timezone(timedelta(hours=8))) + timedelta(days=1)).replace(hour=23, minute=59,second=59).strftime("%Y-%m-%d %I:%M:%S%p"))'''
