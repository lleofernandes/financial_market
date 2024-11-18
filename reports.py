import pandas as pd
import os

report_dir = "reports"
consolidated_excel = f"{report_dir}/ativos_resultado.xlsx"

# Verifica se a pasta "reports" existe, caso contrário, cria a pasta
if not os.path.exists(report_dir):
    os.makedirs(report_dir)

def create_reports(all_data):
    """
    Função para consolidar os dados e exportar para um arquivo Excel.
    
    Parametros:
    - all_data: Lista de DataFrames com dados dos ativos
    - report_dir: Caminho do diretório onde o relatório será salvo
    - consolidated_excel: Caminho completo do arquivo Excel
    """

    if all_data:
        # Consolidar os dados
        consolidated_df = pd.concat(all_data, ignore_index=True)
        consolidated_df = consolidated_df.sort_values(by='date_time', ascending=False).reset_index(drop=True)
        
        # Exporta os dados consolidados para Excel
        consolidated_df.to_excel(consolidated_excel, index=False)
        print(f"Dados consolidados exportados para {consolidated_excel}")
    else:
        print("Nenhum dado disponível para exportar")
