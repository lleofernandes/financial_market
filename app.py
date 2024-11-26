import MetaTrader5 as mt5
import json
import pytz
import os
from datetime import datetime, timedelta
from graphics import create_graphics
from reports import create_reports
from etl import proccess_ativos
from custom_class import custom_trader


#Configuração de timezone e datas
timezone = pytz.timezone('America/Sao_Paulo') #timezone local

# As datas de processamento para o arquivo CSV
current_year = datetime.now().year
initial_date = datetime(current_year - 6, 1, 1) #data fixa dos ultimos 6 anos
final_date = datetime.now()


# timeframe = 'TIMEFRAME_M1'

timeframe_list = ['TIMEFRAME_H1',]

# timeframe_list = [
#     'TIMEFRAME_M1', 'TIMEFRAME_M2', 'TIMEFRAME_M3', 'TIMEFRAME_M4', 'TIMEFRAME_M5', 'TIMEFRAME_M6', 'TIMEFRAME_M10', 'TIMEFRAME_M12', 'TIMEFRAME_M15', 'TIMEFRAME_M20', 'TIMEFRAME_M30',
#     'TIMEFRAME_H1', 'TIMEFRAME_H2', 'TIMEFRAME_H3', 'TIMEFRAME_H4', 'TIMEFRAME_H6', 'TIMEFRAME_H8', 'TIMEFRAME_H12',
#     'TIMEFRAME_D1',
#     'TIMEFRAME_W1',
#     'TIMEFRAME_MN1'
# ]  


#Lista de ativos
# ativos = ["MXRF11",]
ativos = ["AIEC11", "BTLG11", "MXRF11", "XPML11", "PETR3"]


def main():    

    try:
        end_date = datetime.now(timezone)
        start_date = end_date - timedelta(days=30)

        file_path = os.path.join(os.getcwd(), 'credentials.json')
        trader = custom_trader(file_path=file_path)
        
        
        # Atualizando OHLC e Ticks para os ativos
        for timeframe in timeframe_list:
            print(f"Iniciando atualização de dados para o timeframe {timeframe}")
            trader.update_ohlc(ativos, timeframe=timeframe, start_date=initial_date, end_date=final_date, timezone=timezone)
            trader.update_ticks(ativos, start_date=initial_date, end_date=final_date, timezone=timezone)
        
        # Flag para indicar que a atualização de dados foi concluída
        print("Atualização de dados concluída. Iniciando processamento de dados para ETL.")
        
        #Chama a funcao de ETL para processar os dados
        all_data = proccess_ativos(ativos, start_date, end_date, timezone)
                                 
    
        if all_data:
            #GRAFICOS -------
            fig = create_graphics(all_data, ativos)
            fig.show()

            #REPORTS -------
            create_reports(all_data)

        else:
            print("Nenhum dado disponível para exportar")


    except FileNotFoundError:
        print("Arquivo 'credentials.json' não foi encontrado")

    except KeyError as e:
        print(f'A chave {e} não foi encontada no arquivo JSON')

    except ValueError as e:
        print(f"Erro relacionado aos dados: {e}")

    except Exception as e:
        print(f'Ocorreu um erro inesperado: {e}')

    finally:
        mt5.shutdown()

if __name__ == "__main__":
    main()