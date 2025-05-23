import requests
import pandas as pd
from datetime import datetime
from typing import Dict, Optional

# Mapeamento dos indicadores e seus códigos SGS
indicadores = {
    "IPCA": 43,
    "SELIC": 432,
    "PIB": 4380,
    "DÓLAR": 1,
    "COMMODITIES": 22795,
    "IGP-M": 189,
}


def coletar_indicadores_bacen(indicadores: Dict[str, str], n_ultimos: int = 20) -> Optional[pd.DataFrame]:
    """
    Coleta indicadores econômicos da API do Banco Central do Brasil.

    Args:
        indicadores: Dicionário com nome do indicador como chave e código como valor
        n_ultimos: Número de últimos registros a serem coletados (padrão: 20)

    Returns:
        DataFrame com os dados coletados ou None se nenhum dado for obtido
    """
    # Lista para armazenar todos os DataFrames coletados
    todos_dados = []

    # Itera sobre cada indicador fornecido
    for nome, codigo in indicadores.items():
        # Constrói a URL da API do BACEN com o código do indicador
        url = f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo}/dados/ultimos/{n_ultimos}?formato=json"

        try:
            # Faz a requisição HTTP com timeout de 30 segundos
            response = requests.get(url, timeout=30)
            # Levanta exceção se status HTTP indica erro
            response.raise_for_status()

            # Converte resposta JSON para dicionário Python
            dados = response.json()

            # Verifica se a API retornou dados válidos
            if not dados:
                print(f"Nenhum dado encontrado para {nome} (código {codigo})")
                continue

            # Cria DataFrame com os dados retornados
            df = pd.DataFrame(dados)
            # Converte valores de string para float (substitui vírgula por ponto)
            df["valor"] = df["valor"].str.replace(",", ".").astype(float)
            # Adiciona coluna com nome do indicador para identificação
            df["indicador"] = nome
            # Adiciona timestamp da coleta
            df["data_coleta"] = datetime.now().date()
            # Adiciona DataFrame à lista de dados coletados
            todos_dados.append(df)

        except requests.exceptions.RequestException as e:
            # Captura erros de rede/HTTP
            print(f"Erro ao buscar {nome} (código {codigo}): {e}")
        except Exception as e:
            # Captura outros erros não previstos
            print(f"Erro inesperado para {nome} (código {codigo}): {e}")

    # Verifica se pelo menos um indicador foi coletado com sucesso
    if not todos_dados:
        return None

    # Concatena todos os DataFrames em um único DataFrame final
    df_final = pd.concat(todos_dados, ignore_index=True)
    return df_final


# Teste da função com os indicadores definidos
df_indicadores = coletar_indicadores_bacen(indicadores)
df_indicadores.to_csv("../data/indicadores_economicos.csv", index=False, encoding="utf-8-sig")
print("Dados coletados e salvos em 'data/indicadores_economicos.csv'")
