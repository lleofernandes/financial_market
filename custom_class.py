import MetaTrader5 as mt5
import json
from datetime import datetime, timedelta
import pandas as pd
import os

from etl import proccess_ativos


initial_date = datetime(2020, 1, 1) #data fixa iniciando em 2020-1-1
final_date = datetime.now()


def initialize_directories(base_path, subfolders):
    for folder in subfolders:
        os.makedirs(os.path.join(base_path, folder), exist_ok=True)

class custom_trader():
    def __init__(self, file_path=None, login=None, password=None, server=None):
        if file_path:
            try:
                with open(file_path) as f:
                    credentials = json.load(f)
                self.server = credentials['mt_server']
                self.login = credentials['mt_login']
                self.password = credentials['mt_pswrd']
                
            except:
                print('Erro ao ler as credenciais')
                quit()
        
        else:
            self.server = server
            self.login = login
            self.password = password
            if (login and password and server) == None: 
                print("Erro ao ler as credenciais")
                quit()
            
        #caso o mt5 não inicialize, quit()
        if not mt5.initialize(login=self.login, password=self.password, server=self.server):
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
            'TIMEFRAME_MN1': [mt5.TIMEFRAME_MN1, 2592000],         
        }


        initialize_directories('ohlc', self.timeframe_dict.keys())
        initialize_directories('', ['ticks'])
        
        # for folder in ['ohlc', 'ticks']:
        #     os.makedirs(folder, exist_ok=True)

        # for timeframe in self.timeframe_dict.keys():
        #     os.makedirs(f"ohlc\\{timeframe}", exist_ok=True)       
        
        # if not os.path.isdir('ohlc'):
        #     os.mkdir('ohlc')
        #     for timeframe in self.timeframe_dict.keys():
        #         try:
        #             os.mkdir(f"ohlc\\{timeframe}")
        #         except FileExistsError:
        #             pass
        # elif not os.path.isdir('ticks'):
        #     os.mkdir('ticks')

    # funcoes ================
    def update_ohlc(self, ativos, timeframe, start_date, end_date, timezone=None):
        """
        Atualiza os dados OHLC para uma lista de ativos em um timeframe específico.
        """

        # Processa os ativos utilizando a função do ETL
        all_data = proccess_ativos(ativos, start_date, end_date, timezone)

        for df in all_data:
            symbol = df['symbol'].iloc[0]
            timeframe_path = f"ohlc\\{timeframe}\\{symbol}_{timeframe}.csv"

            # Verifica se o arquivo já existe
            if os.path.exists(timeframe_path):
                df_old = pd.read_csv(timeframe_path, parse_dates=['date_time'])
                # df_old['date_time'] = pd.to_datetime(df_old['date_time'])
                df = pd.concat([df, df_old], ignore_index=True)
                df.drop_duplicates(subset=['date_time', 'symbol'], inplace=True)

             # Ordena e salva o resultado
            df.sort_values(by='date_time', ascending=False, inplace=True)
            df.to_csv(timeframe_path, index=False)
            print(f"Dados atualizados para {symbol} no timeframe {timeframe}.")


        if not os.path.exists(f"ohlc\\{timeframe}\\{symbol}_{timeframe}.csv"):
            df = pd.DataFrame(columns=['date_time', 'open', 'high', 'low', 'close', 'tick_volume', 'spread', 'real_volume'])
        
        else:
            df = pd.read_csv(f"ohlc\\{timeframe}\\{symbol}_{timeframe}.csv")
            df['date_time'] = pd.to_datetime(df['date_time'])
            if df['date_time'].max() < datetime.now() - timedelta(days=7):
                initial_date = df['date_time'].max()
            else: 
                return
            
        timedelta_default = timedelta(days=self.timeframe_dict[timeframe][1])
        final_date_aux = initial_date + timedelta_default
        timeframe_name = timeframe
        timeframe = self.timeframe_dict[timeframe][0]

        dfs = []
        while True:
            data_aux = mt5.copy_rates_range(symbol, timeframe, initial_date, min(final_date_aux, final_date))
            if data_aux is not None:
                df_aux = pd.DataFrame(data_aux)
                df_aux['date_time'] = pd.to_datetime(df_aux['date_time'], unit='s')
                dfs.append(df_aux)


            if final_date_aux > final_date:
                break
            
            # if data_aux is not None and len(data_aux) > 0:
            initial_date = pd.to_datetime(data_aux['date_time'].max(), unit='s')
            final_date_aux = initial_date + timedelta_default


        if not df_aux.empty:
            df = pd.concat(dfs, ignore_index=True)

        df.sort_values(by='date_time', ascending=False, inplace=True)
        df.to_csv(f"ohlc\\{timeframe_name}\\{symbol}_{timeframe_name}.csv", index=False)

    def update_ticks(self, symbol):
        if not os.path.exists(f"ticks\\{symbol}_ticksrange.csv"):
            df = pd.DataFrame(columns=['date_time', 'bid', 'ask', 'last', 'volume', 'time_msc', 'flags', 'volume_real'])
        
        else:
            df = pd.read_csv(f"ticks\\{symbol}_ticksrange.csv")
            df['date_time'] = pd.to_datetime(df['date_time'])
            if df['date_time'].max() < datetime.now() - timedelta(days=7):
                initial_date = df['date_time'].max()
        
        ticks_data = mt5.copy_ticks_range(symbol, initial_date, final_date, mt5.COPY_TICKS_TRADE)
        df_aux = pd.DataFrame(ticks_data)
        df_aux['date_time'] = pd.to_datetime(df_aux['date_time'])
        if not df_aux.empty:
            df = pd.concat([df_aux, df], ignore_index=True)
        df['date_time'] = pd.to_datetime(df['date_time'])

        df.sort_values(by='date_time', ascending=False, inplace=True)
        df.to_csv(f"ticks\\{symbol}_ticksrange.csv", index=False)

    def slice(self, type, symbol, initial_date, final_date, timeframe=None):
        path = f'ohlc\\{timeframe}\\{symbol}_{timeframe}.csv' if type=='ohlc' else f'ticks\\{symbol}_ticksrage.csv'
        
        if not os.path.exists(path):
            print(f'O ativo {symbol} não está registrado, crie utilizando a função o .update_{type}')
        else:
            df = pd.read_csv(path)
            df['date_time'] = pd.to_datetime(df['date_time'])
            return df.loc[(df['date_time'] >= initial_date) & (df['date_time'] < final_date)]
        pass
        

    def read_ohlc(self, symbol, timeframe, initial_date=datetime(2020, 1, 1), final_date=datetime.now()):
        return self.slice('ohlc', symbol, initial_date, final_date, timeframe)
        pass
        

    def read_ticks(self, symbol, initial_date=datetime(2020, 1, 1), final_date=datetime.now()):
        return self.slice('ticks', symbol, initial_date, final_date)
        pass
        


