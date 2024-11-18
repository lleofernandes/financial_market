import MetaTrader5 as mt5
import json
from datetime import datetime, timedelta
import pandas as pd
import os

from etl import proccess_ativos

current_year = datetime.now().year
initial_date = datetime(current_year - 6, 1, 1) #data fixa dos ultimos 5 anos
final_date = datetime.now()


def initialize_directories(base_path, subfolders):
    for folder in subfolders:
        os.makedirs(os.path.join(base_path, folder), exist_ok=True)

class custom_trader():
    def __init__(self, file_path=None, login=None, password=None, server=None):
        #carrega as credenciais de arquivo ou parametros
        self._load_credentials(file_path, login, password, server)
        
        # Inicializa a conexão com o MetaTrader
        self._initialize_mt5() 
            

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

    
    def _load_credentials(self, file_path, login, password, server):
        if file_path:
            try:
                with open(file_path) as f:
                    credentials = json.load(f)
                self.server = credentials['mt_server']
                self.login = credentials['mt_login']
                self.password = credentials['mt_pswrd']
                
            except Exception as e:
                print(f'Erro ao ler as credenciais: {e}')
                quit()
        
        else:
            self.server = server
            self.login = login
            self.password = password
            if not all([self.login, self.password, self.server]): 
                print("Erro ao ler as credenciais")
                quit()

    def _initialize_mt5(self):
        #caso o mt5 não inicialize, quit()
        if not mt5.initialize(login=self.login, password=self.password, server=self.server):
            print(f'initialize() failed, error code: {mt5.last_error()}')
            mt5.shutdown()
            quit()

    def _update_data(self, symbol, df, timeframe, data_type='ohlc'):
        """Função auxiliar para atualizar os arquivos de dados (OHLC ou Ticks)."""
        if data_type == 'ohlc':
            timeframe_path = f"ohlc\\{timeframe}\\{symbol}_{timeframe}.csv"
        else:
            timeframe_path = f"ticks\\{symbol}_ticksrange.csv"

        # Verifica se o arquivo já existe
        if os.path.exists(timeframe_path):
             # Lê o arquivo CSV existente
            df_old = pd.read_csv(timeframe_path, parse_dates=['date_time'])

            # Encontra a última data registrada no arquivo existente
            last_date_in_file = df_old['date_time'].max()

            # Filtra os dados novos para incluir apenas os que são posteriores à última data no arquivo
            df_new = df[df['date_time'] > last_date_in_file]
            if not df_new.empty:
                # Concatena os novos dados ao antigo
                df_combined = pd.concat([df_old, df_new], ignore_index=True).drop_duplicates(subset=['date_time', 'symbol'], keep='last')
                df_combined.sort_values(by='date_time', ascending=False, inplace=True)

                # Atualiza o arquivo CSV com os dados combinados
                df_combined.to_csv(timeframe_path, index=False)
                print(f"Novos dados adicionados ao arquivo {timeframe_path} para {symbol} para op timeframe {timeframe}.")
            else:
                print(f"Não há novos dados para {symbol} após {last_date_in_file}.")

        else:
            # Se o arquivo não existir, cria o arquivo e escreve os dados
            df.sort_values(by='date_time', ascending=False, inplace=True)
            df.to_csv(timeframe_path, index=False)
            print(f"Arquivo criado para {symbol} no tipo {data_type} para o timeframe {timeframe} no arquivo .csv.")

        

    # funcoes ================
    def update_ohlc(self, ativos, timeframe, start_date=None, end_date=None, timezone=None):
        """
        Atualiza os dados OHLC para uma lista de ativos em um timeframe específico.
        """

        start_date = initial_date
        end_date = final_date


        # Processa os ativos utilizando a função do ETL
        all_data = proccess_ativos(ativos, start_date, end_date, timezone)

        for df in all_data:
            symbol = df['symbol'].iloc[0]
            self._update_data(symbol, df, timeframe, data_type='ohlc')
            

    def update_ticks(self, ativos, start_date=None, end_date=None, timezone=None):
        """
        Atualiza os dados TICKS para uma lista de ativos em um timeframe específico.
        """
        start_date = initial_date
        end_date = final_date

        all_data = proccess_ativos(ativos, start_date, end_date, timezone)

        for df in all_data:
            symbol = df['symbol'].iloc[0]
            self._update_data(symbol, df, timeframe=None, data_type='ticks')
                   

    def slice(self, data_type, symbol, initial_date, final_date, timeframe=None):
        """Função genérica para ler os dados de OHLC ou Ticks de acordo com o tipo especificado."""
        path = f'ohlc\\{timeframe}\\{symbol}_{timeframe}.csv' if data_type=='ohlc' else f'ticks\\{symbol}_ticksrange.csv'
        
        if not os.path.exists(path):
            print(f'O ativo {symbol} não está registrado, crie utilizando a função o .update_{type}')
        else:
            df = pd.read_csv(path)
            df['date_time'] = pd.to_datetime(df['date_time'])
            return df.loc[(df['date_time'] >= initial_date) & (df['date_time'] < final_date)]
        
        

    def read_ohlc(self, symbol, timeframe, initial_date=initial_date, final_date=datetime.now()):
        return self.slice('ohlc', symbol, initial_date, final_date, timeframe)
        
        

    def read_ticks(self, symbol, initial_date=initial_date, final_date=datetime.now()):
        return self.slice('ticks', symbol, initial_date, final_date)
        
        


