import MetaTrader5 as mt5
import json
from datetime import datetime, timedelta
import pandas as pd
import os

class btoolsTrader():
    def __init__(self, file_path=None, login=None, pswrd=None, server=None):
        if file_path:
            try:
                with open(file_path) as f:
                    credentials = json.load(f)
                self.server = credentials['mt_server']
                self.login = credentials['mt_login']
                self.pswrd = credentials['mt_pswrd']
                
            except:
                print('Erro ao ler as credenciais')
                quit()
        
        else:
            self.server = server
            self.login = login
            self.pswrd = pswrd
            if (login and pswrd and server) == None: 
                print("Erro ao ler as credenciais")
                quit()
            
        #caso o mt5 não inicialize, quit()
        if not mt5.initialize(login=self.login, pswrd=self.pswrd, server=self.server):
            print(f'initialize() failed, error code: {mt5.last_error()}')
            mt5.shutdown()
            quit()

        self.timeframe_dict = {
            'TIMEFRAME_M1': [mt5.TIMEFRAME_M1, 60], #o valor 60 é tempo máximo que consegue pegar em dias
            'TIMEFRAME_M2': [mt5.TIMEFRAME_M2, 120],
            'TIMEFRAME_M3': [mt5.TIMEFRAME_M3, 180],
            'TIMEFRAME_M4': [mt5.TIMEFRAME_M4, 240],
            'TIMEFRAME_M5': [mt5.TIMEFRAME_M5, 300],
            'TIMEFRAME_M6': [mt5.TIMEFRAME_M6, 360],
            'TIMEFRAME_M10': [mt5.TIMEFRAME_M10, 600],
            'TIMEFRAME_M12': [mt5.TIMEFRAME_M12, 720],
            'TIMEFRAME_M15': [mt5.TIMEFRAME_M15, 900],
            'TIMEFRAME_M20': [mt5.TIMEFRAME_M20, 1200],
            'TIMEFRAME_M30': [mt5.TIMEFRAME_M30, 1800],
            'TIMEFRAME_H1': [mt5.TIMEFRAME_H1, 3600],
            'TIMEFRAME_H2': [mt5.TIMEFRAME_H2, 7200],
            'TIMEFRAME_H3': [mt5.TIMEFRAME_H3, 10800],
            'TIMEFRAME_H4': [mt5.TIMEFRAME_H4, 14400],
            'TIMEFRAME_H6': [mt5.TIMEFRAME_H6, 21600],
            'TIMEFRAME_H8': [mt5.TIMEFRAME_H8, 28800],
            'TIMEFRAME_H12': [mt5.TIMEFRAME_H12, 43200],
            'TIMEFRAME_D1': [mt5.TIMEFRAME_D1, 86400],
            'TIMEFRAME_W1': [mt5.TIMEFRAME_W1, 604800],
            'TIMEFRAME_WN1': [mt5.TIMEFRAME_WN1, 2592000],         
        }

        if not os.path.isdir('ohlc'):
            os.mkdir('ohlc')
            for timeframe in self.timeframe_dict.keys():
                try:
                    os.mkdir(f"ohlc\\{timeframe}")
                except FileExistsError:
                    pass
        elif not os.path.isdir('ticks'):
            os.mkdir('ticks')

    # funcoes ================
    def update_ohlc(self, symbol, timeframe):
        initial_date = datetime(2020, 1, 1) #data fixa iniciando por 2020
        final_date = datetime.now()

        if not os.path.exists(f"ohlc\\{timeframe}\\{symbol}_{timeframe}.csv"):
            df = pd.DateFrame(columns=['time', 'open', 'high', 'low', 'close', 'tick_volume', 'spread', 'real_volume'])
        
        else:
            df = pd.read_csv(f"ohlc\\{timeframe}\\{symbol}_{timeframe}.csv")
            df['time'] = pd.to_datetime(df['time'], unit='s')
            if df['time'].max() < datetime.now() - timedelta(days=7):
                initial_date = df['time'].max()
            else: 
                return
            
        timedelta_default = timedelta(days=self.timeframe_dict[timeframe][1])
        final_date_aux = initial_date + timedelta_default
        timeframe_name = timeframe
        timeframe = self.timeframe_dict[timeframe][0]

        while True:
            data_aux = mt5.copy_rates_ranges(symbol, timeframe, initial_date, min(final_date_aux, final_date))
            df_aux = pd.DataFrame(data_aux)
            df_aux['time'] = pd.to_datetime(df_aux['time'], unit='s')
            df = pd.concat([df_aux, df], ignore_index=True)

            if final_date_aux > final_date:
                break

            initial_date = data_aux['time'].max()
            final_date_aux = initial_date + timedelta_default

        df.sort_values(by='time', ascending=False, inplace=True)
        df.to_csv(f"ohlc\\{timeframe_name}\\{symbol}_{timeframe_name}.csv", index=False)
