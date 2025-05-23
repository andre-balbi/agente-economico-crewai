import requests
import pandas as pd
from time import sleep
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()
api_key = os.getenv("ALPHA_VANTAGE_API_KEY")

# Top 10 ações da B3 por volume - definidas manualmente
top_10_acoes = ["PETR4", "VALE3", "ITUB4", "BBDC4", "ABEV3", "BBAS3", "B3SA3", "WEGE3", "RENT3", "MGLU3"]


def buscar_dados_acao_alpha_vantage(ticker_b3, api_key, num_registros=10):
    """
    Busca dados históricos de ações da B3 usando a API Alpha Vantage.

    Args:
        ticker_b3: Código da ação na B3 (ex: 'PETR4')
        api_key: Chave da API Alpha Vantage
        num_registros: Número de registros mais recentes a retornar (padrão: 10)

    Returns:
        DataFrame com dados da ação ou None em caso de erro
    """
    # Adiciona sufixo .SA para ações brasileiras na Alpha Vantage
    ticker = ticker_b3 + ".SA"

    # Constrói URL da API - outputsize=compact busca ~100 pontos mais recentes
    url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={ticker}&apikey={api_key}&outputsize=compact"

    # Faz primeira tentativa de requisição
    response = requests.get(url)

    # Verifica se houve erro na requisição
    if response.status_code != 200:
        # Tratamento específico para erro 503 (servidor indisponível)
        if response.status_code == 503:
            print(f"[{ticker_b3}] Servidor Alpha Vantage indisponível (503). Tentando novamente em alguns segundos...")
            sleep(30)  # Espera 30 segundos antes de tentar novamente
            response = requests.get(url)  # Segunda tentativa

            # Se ainda der erro após segunda tentativa
            if response.status_code != 200:
                raise Exception(f"[{ticker_b3}] Erro {response.status_code} após nova tentativa.")
        else:
            # Para outros códigos de erro
            raise Exception(f"[{ticker_b3}] Erro {response.status_code}")

    # Converte resposta JSON para dicionário Python
    data = response.json()

    # Verifica se há mensagens de limite de API ou erro
    if "Note" in data or "Information" in data:
        print(
            f"[{ticker_b3}] Nota da API: {data.get('Note', data.get('Information', 'Limite de API provavelmente atingido.'))}"
        )
        return None

    # Verifica se os dados de série temporal estão presentes
    if "Time Series (Daily)" not in data:
        print(f"[{ticker_b3}] Sem dados 'Time Series (Daily)' na resposta. Resposta completa: {data}")
        return None

    # Cria DataFrame a partir dos dados da série temporal
    df = pd.DataFrame.from_dict(data["Time Series (Daily)"], orient="index")

    # Renomeia colunas para português
    df.columns = ["abertura", "alta", "baixa", "fechamento", "volume"]

    # Converte todas as colunas para float
    df = df.astype(float)

    # Converte índice (datas) para datetime
    df.index = pd.to_datetime(df.index)

    # Ordena por data (mais antiga para mais recente)
    df = df.sort_index(ascending=True)

    # Adiciona coluna com ticker original da B3
    df["ticker"] = ticker_b3

    # Seleciona apenas os últimos N registros (mais recentes)
    df = df.tail(num_registros)

    return df


# DataFrame para armazenar dados de todos os ativos
df_total = pd.DataFrame()

# Itera sobre cada ativo da lista dos top 10
for ativo in top_10_acoes:
    try:
        print(f"Coletando {ativo}...")

        # Busca dados do ativo usando a API Alpha Vantage
        # Por padrão busca 20 registros (pode ser alterado passando num_registros)
        df = buscar_dados_acao_alpha_vantage(ativo, api_key)
        # Exemplo para número diferente: buscar_dados_acao_alpha_vantage(ativo, api_key, num_registros=25)

        # Verifica se os dados foram coletados com sucesso e não estão vazios
        if df is not None and not df.empty:
            # Adiciona dados do ativo ao DataFrame total
            df_total = pd.concat([df_total, df])
            print(f"✓ {ativo} adicionado com {len(df)} registros.")

        elif df is not None and df.empty:
            # DataFrame existe mas está vazio (sem dados após processamento)
            print(
                f"⚠ {ativo} retornou um DataFrame vazio após o processamento (pode ser que não haja dados suficientes após o filtro)."
            )

        # Se df for None, a mensagem de erro já foi impressa dentro da função

        # Pausa para respeitar limites da API Alpha Vantage (5 chamadas/minuto, 500/dia na versão gratuita)
        sleep(15)

    except Exception as e:
        # Captura qualquer erro durante o processamento do ativo
        print(f"✗ Erro com {ativo}: {e}")

# Salva dados coletados em arquivo CSV
if not df_total.empty:
    # Salva com índice (datas) e encoding UTF-8 com BOM para Excel
    df_total.to_csv("../data/top_10_acoes.csv", index=True, encoding="utf-8-sig")
    print(f"📁 Arquivo final salvo com {len(df_total)} linhas.")
else:
    # Nenhum dado foi coletado com sucesso
    print("⚠ Nenhum dado foi coletado para salvar no arquivo CSV.")
