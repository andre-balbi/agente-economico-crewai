import requests
from bs4 import BeautifulSoup
import pandas as pd

palavras_chave = [
    "ipca",
    "inflação",
    "selic",
    "juros",
    "bovespa",
    "ações",
    "investimentos",
    "bolsa",
    "ibovespa",
    "economia",
    "mercado",
    "taxa básica",
    "taxa de juros",
]

headers = {"User-Agent": "Mozilla/5.0"}

sites = {
    "CNN Brasil": "https://www.cnnbrasil.com.br/economia/",
    "G1 Economia": "https://g1.globo.com/economia/",
    "InfoMoney": "https://www.infomoney.com.br/mercados/",
    "Exame Economia": "https://exame.com/economia/",
}

# Lista global para armazenar todas as notícias coletadas
noticias = []


def filtrar_noticias(html, base_url):
    """Filtra notícias do HTML baseado em palavras-chave"""
    # Cria objeto BeautifulSoup para parsing do HTML
    soup = BeautifulSoup(html, "html.parser")
    encontrados = []

    # Itera sobre todos os links encontrados na página
    for a in soup.find_all("a", href=True):
        # Extrai e normaliza o texto do título
        titulo = a.get_text().strip().lower()
        # Obtém o link/URL
        link = a["href"]

        # Verifica se alguma palavra-chave está presente no título
        if any(p in titulo for p in palavras_chave):
            # Processa apenas títulos não vazios com links HTTP absolutos
            if titulo and link.startswith("http"):
                encontrados.append({"titulo": titulo.title(), "link": link})
            # Processa títulos com links relativos (começam com "/")
            elif titulo and link.startswith("/"):
                encontrados.append({"titulo": titulo.title(), "link": base_url + link})

    return encontrados


# Loop principal para processar cada site da lista
for nome_site, url in sites.items():
    try:
        # Faz requisição HTTP para o site com timeout de 10 segundos
        resp = requests.get(url, headers=headers, timeout=10)

        # Verifica se a requisição foi bem-sucedida (status 200)
        if resp.status_code == 200:
            # Extrai URL base (protocolo + domínio) para construir links relativos
            base_url = "/".join(url.split("/")[:3])
            # Filtra notícias da página e adiciona à lista global
            noticias += filtrar_noticias(resp.text, base_url)
        else:
            # Informa erro de status HTTP diferente de 200
            print(f"[!] Erro ao acessar {nome_site}: Status {resp.status_code}")

    except Exception as e:
        # Captura qualquer exceção durante o processamento do site
        print(f"[!] Falha ao acessar {nome_site}: {e}")


# Remove notícias duplicadas baseado no título
# Usa dict comprehension para manter apenas a primeira ocorrência de cada título
noticias_unicas = list({n["titulo"]: n for n in noticias}.values())

# Converte lista de dicionários para DataFrame do pandas
df = pd.DataFrame(noticias_unicas)
