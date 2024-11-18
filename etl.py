import MetaTrader5 as mt5
import pandas as pd
import os



def proccess_ativos(ativos, start_date, end_date, timezone):
    all_data = []
    end_date_format = pd.to_datetime(end_date).date()
    start_date_format = pd.to_datetime(start_date).date()

    for ativo in ativos:
        # print(f"Analisando ativo: {ativo}")
        symbol_info = mt5.symbol_info(ativo)
        if symbol_info is None:
            print(f"Ativo {ativo} não encontrado no MetaTrader.")
            # raise ValueError(f"O ativo {ativo} não está disponível no MetaTrader.")
            continue

        #obter os dados históricos do ativo
        print(f"Atualização de dados para o ativo {ativo} de {start_date_format} a {end_date_format} processados com sucesso")
        dataMT = mt5.copy_rates_range(ativo,
                                    mt5.TIMEFRAME_H1,
                                    start_date,
                                    end_date
                                    )

        if dataMT is None:
            print(f"Erro ao obter dados para o ativo {ativo}: {mt5.last_error()}")
            # raise ValueError(f"Não foi possível obter dados para o ativo {ativo}. Erro: {mt5.last_error()} ")
            continue

        if len(dataMT) == 0:
            print(f"Nenhum dado retornado para o ativo {ativo}. Verifique se o ativo está disponível no MetaTrader.")
            continue

        # Processar os dados obtidos
        df = pd.DataFrame(dataMT)
        # print(f"Dados para o ativo {ativo} antes da conversão: \n{df.head()}")
        df['time'] = pd.to_datetime(df['time'], unit='s') #converte a coluna 'time' de UNIX timestamp para datetime
        df.rename(columns={'time': 'date_time'}, inplace=True)
        df['date_time'] = pd.to_datetime(df['date_time'])
        
        # Ordenar os dados e adicionar colunas
        df = df.sort_values(by='date_time', ascending=False).reset_index(drop=True)
        df['symbol'] = ativo #adiciona uma coluna com o nome de ativo
        df['verified_at'] = end_date.replace(tzinfo=None)

        # Adicionar os dados ao report consolidado
        all_data.append(df)
        # print(f"Dados para {ativo} processados com sucesso")
    
    return all_data
    
     