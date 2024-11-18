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
end_date = datetime.now(timezone)
start_date = end_date - timedelta(days=7)

# As datas de processamento para o arquivo CSV
initial_date = datetime(2020, 1, 1) #data fixa iniciando em 2020-1-1
final_date = datetime.now()


timeframe = 'TIMEFRAME_M1'

#Lista de ativos
ativos = ["AIEC11", "BTLG11", "MXRF11", "XPML11", ]


def main():
    try:
        file_path = os.path.join(os.getcwd(), 'credentials.json')
        trader = custom_trader(file_path=file_path)
        
        # trader.update_ohlc(symbol='PETR3', timeframe='TIMEFRAME_M1')
        trader.update_ohlc(ativos, timeframe, initial_date, final_date, timezone)
        trader.update_ticks(ativos)
        

        # with open('credentials.json') as f:
        #     data = json.load(f)            

        # #inicializa a aplicação e valida os dados de acesso
        # if not mt5.initialize(login=data['mt_login'], password=data['mt_pswrd'], server=data['mt_server'], timeout=10000): #timeout é em milissegundos
        # # if not mt5.initialize(login=data['mt_login'], password=data['mt_pswrd'], server=data['mt_server'], timeout=10000): #timeout é em milissegundos
        #     print(f'initialize() failed, error code: {mt5.last_error()}')
        #     mt5.shutdown()
        #     raise SystemExit("Erro ao inicializar o MetaTrader 5. Verifique as credenciasis e o servidor.")
        
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