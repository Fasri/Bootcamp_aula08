import pandas as pd
import os
import glob

""" Função de extração de dados que le e consolida os jsons """


def extract_data(path: str) -> pd.DataFrame:
    arquivos = glob.glob(os.path.join(path, '*.json'))
    df_list = [pd.read_json(arquivo) for arquivo in arquivos]
    df_total = pd.concat(df_list, ignore_index=True)
    return df_total

#Transforma de dados

def calcular_kpi_de_tota_de_vendas(df: pd.DataFrame) -> pd.DataFrame:
    df['Total de Vendas'] = df['Quantidade'] * df['Venda']
    return df

# Funçao para carregar em csv ou parquet

def carregar(df: pd.DataFrame, format_out: list) -> None:

    for format in format_out:
        if format == 'csv':
            df.to_csv('data.csv', index=False)
        elif format == 'parquet':
            df.to_parquet('data.parquet', index=False)

# Função para calcular o total de vendas

def consolidado(pasta: str, format_out: list) -> None:
    df = extract_data(pasta)
    df = calcular_kpi_de_tota_de_vendas(df)    
    carregar(df,format_out)
  