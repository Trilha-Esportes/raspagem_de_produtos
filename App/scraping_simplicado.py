import cloudscraper
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
import time
import random
import requests
from decimal import Decimal, InvalidOperation
import re
from requests.exceptions import HTTPError

agent = UserAgent()
scraper = cloudscraper.create_scraper()

PRODUTO_OFF = {
                        "link_ativo": False,
                        "nome_produto": "",
                        "preco_produto": 0.0,
                        "vendedor": "",
                        "tag_sem_estoque": False,
                        "tag_ultimas_unidades": False,
                        "descricao_erro": "Link fora do ar"
                    }

def delay():
    """Intervalo aleatório entre as requisições para evitar bloqueio"""
    time.sleep(random.uniform(10, 15))

def get_html(url):
    """Obtém o HTML da página usando headers realistas e cloudscraper"""
    headers = {
        "User-Agent": agent.random,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
        "Referer": "https://www.google.com/",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
    }


    try:
        response = scraper.get(url, headers=headers, timeout=15)
        response.raise_for_status()

        # Verificação adicional contra bloqueios disfarçados
        if (
            "Access Denied" in response.text
            or "cloudflare" in response.text.lower()
            or "bot verification" in response.text.lower()
        ):
            return "ACCESS_DENIED"

        return response.text

    except HTTPError as e:
        if response.status_code == 404:
            print(f"Página não encontrada: {url}")
            return "Not Found"

    except requests.exceptions.RequestException as e:
        print(f"Erro ao acessar {url}: {str(e)}")
        return None

def extrair_dados(html):
    """Extrai os dados relevantes do HTML do produto"""
    resultado = {
        "link_ativo": True,
        "nome_produto": "",
        "preco_produto": 0.0,
        "vendedor": "",
        "tag_sem_estoque": False,
        "tag_ultimas_unidades": False,
        "descricao_erro": ""
    }

    soup = BeautifulSoup(html, "html.parser")

    try:
        title = soup.title.string.strip() if soup.title else ""

        # Produto fora do ar (não existe mais ou foi removido)
        if "Produto Temporariamente Indisponivel" in title:
            resultado["link_ativo"] = False
            resultado["descricao_erro"] = "Produto fora do ar"
            return resultado

        # Produto existente, mas sem estoque
        if (
            soup.find("div", {"data-testid": "content-unavailable"})
            or "Produto indisponível" in html
        ):
            resultado["tag_sem_estoque"] = True

        # Produto ativo (mesmo que sem estoque)
        resultado["nome_produto"] = title.replace(" | Centauro", "").strip()


        # Só extrair detalhes se o produto estiver com página ativa
        if resultado["link_ativo"]:
            # Preço
            preco_tag = soup.select_one('[data-testid="price-current"]')
            if preco_tag:
                resultado["preco_produto"] = formatar_preco(preco_tag.text.strip())
         
            wrapper = soup.find("div", class_="SocialProofBadges-styled__SocialProofBadgesWrapper-sc-f2e0833c-0 fAtqhl")
            if wrapper:
                span = wrapper.find("span", class_="Tagstyled__Label-sc-aqiv9j-1 eIdNnd")
                if span and "últimas unidades" in span.get_text(strip=True).lower():
                    resultado["tag_ultimas_unidades"] = True



            # Vendedor
            vendedor_tag = soup.select_one('p.Seller-styled__Text-sc-294000f5-0')
            if vendedor_tag:
                texto = vendedor_tag.get_text(strip=True)
                if "Vendido por:" in texto:
                    vendedor = texto.replace("Vendido por:", "").split("e entregue")[0].strip()
                    resultado["vendedor"] = vendedor

    except Exception as e:
        resultado["descricao_erro"] = f"Erro ao extrair dados: {str(e)}"

    return resultado

def coletar_dados_produto(codigo_produto: str) -> dict:
    """Coleta e analisa os dados do produto"""
    delay()
    url = f"https://www.centauro.com.br/{codigo_produto}"
    html = get_html(url)

    if html == "ACCESS_DENIED":
        return "ACCESS_DENIED"
    elif not html:
        return {"status": False, "response": {}}
    elif html == "Not Found":
        dados_produto = PRODUTO_OFF
        return {"status": True, "response": dados_produto}


    dados_produto = extrair_dados(html)
    return {"status": True, "response": dados_produto}



def run(codigo: str, max_tentativas: int = 3) -> dict:
    """Executa a coleta com tentativas e delay em caso de erro"""
    tentativas = 0
    resultado = {'status': False, 'response': {}}

    while tentativas < max_tentativas:
        tentativas += 1
        try:
            resposta = coletar_dados_produto(codigo)

            if resposta != "ACCESS_DENIED" and resposta["status"]:
                resultado.update({
                    'status': True,
                    'response': resposta['response']
                })
                break
            else:
                print(f"Tentativa {tentativas}: acesso negado ou falha.")
                delay()
        except Exception as e:
            print(f"[TENTATIVA {tentativas}] Erro: {str(e)}")
            delay()

    return resultado
def formatar_preco(preco_str: str) -> float:
    """
    Extrai o valor numérico do preço em formato BR ("R$ 199,90")
    e retorna como float: 199.90
    """
    try:
        # Extrai o padrão numérico com vírgula (ex: "199,90")
        match = re.search(r"(\d{1,3}(?:\.\d{3})*,\d{2}|\d+,\d{2})", preco_str)
        if not match:
            return None

        valor_str = match.group(0).replace('.', '').replace(',', '.')
        return float(Decimal(valor_str))

    except (InvalidOperation, ValueError):
        return None


if __name__ == "__main__":
    produto_test = "M13DQDFG0382"
    resultado = run(produto_test)
    

    if resultado['status']:
        print("\n📦 Dados do produto:")
        print(f"🛍️ Ativo: {resultado['response']['link_ativo']}")
        print(f"🛍️ Nome: {resultado['response']['nome_produto']}")
        print(f"💰 Preço: {resultado['response']['preco_produto']}")
        print(f"🏪 Vendedor: {resultado['response']['vendedor']}")
        print(f"⚠️ Últimas unidades: {resultado['response']['tag_ultimas_unidades']}")
        print(f"❌ Sem estoque: {resultado['response']['tag_sem_estoque']}")
    else:
        print("\n❌ Falha ao coletar dados do produto.")
