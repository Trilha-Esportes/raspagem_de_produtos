

from datetime import datetime, date
from sqlalchemy.orm import Session,aliased
from models import *  
from database import SessionLocal, engine, Base
import time
import random
from decimal import Decimal
from sqlalchemy import desc
import psycopg2

from scraping_simplicado  import run , delay
from sqlalchemy import select, func, nullsfirst
from sqlalchemy import asc

import os




def criar_novo_historico(db: Session, numero_de_links: int):
    try:
        # Verifica se já existe um histórico interrompido mais recente
        historico_interrompido = (
            db.query(ScrapingHistorico)
            .filter(ScrapingHistorico.status == "interrompido")
            .order_by(desc(ScrapingHistorico.data_scraping), desc(ScrapingHistorico.inicio_execucao))
            .first()
        )

        if historico_interrompido:
            return historico_interrompido , False

        # Se não existir, cria um novo
        novo_historico = ScrapingHistorico(
            data_scraping=date.today(),
            inicio_execucao=datetime.utcnow(),
            numero_de_links=numero_de_links,
            status="em_andamento"
        )

        db.add(novo_historico)
        db.commit()
        db.refresh(novo_historico)
        return novo_historico , True

    except Exception as e:
        print(f"Erro ao criar histórico: {e}")
        db.rollback()
        raise

def salvar_scraping(db: Session, id_historico: int, id_produto: int, resposta: dict):
   
    
    
    try:    
        scraping = Scraping(
                id_scraping_historico=id_historico,
                id_produto=id_produto,
                link_ativo=resposta.get("link_ativo", None),
                nome_produto=resposta.get("nome_produto", None),
                preco_produto=resposta.get("preco_produto",0),
                vendedor=resposta.get("vendedor", None),
                tag_sem_estoque=resposta.get("tag_sem_estoque", False),
                tag_ultimas_unidades=resposta.get("tag_ultimas_unidades", False),
                descricao_erro=resposta.get("descricao_erro", None),
        )

        

        db.add(scraping)
        db.commit()
        db.refresh(scraping)

        print(f"[DEBUG] Scraping salvo. ID: {scraping.id}")
        return scraping
    except Exception as e:
        print(f"Erro no produto {id_produto}: {e}")
        db.rollback()


def processar_todos_produtos(db: Session):
    TAMANHO_BLOCO = 30
    LIMITE_DA_BUSCA = 250

    # 1. Subconsulta para encontrar a data da última pesquisa de cada produto.
    #    Esta lógica está correta e será usada em ambos os cenários.
    subquery_ultimos_produtos = (
        select(
            Scraping.id_produto,
            func.max(Scraping.data_criacao).label("ultima_execucao")
        )
        .group_by(Scraping.id_produto)
        .subquery()
    )
    s = aliased(subquery_ultimos_produtos)

    # 2. Verifica se existe uma execução para ser retomada.
    historico, novo_historico = criar_novo_historico(db, LIMITE_DA_BUSCA)
    
    # 3. Constrói a query base com a lógica de ordenação correta.
    query_base = (
        select(Produto)
        .outerjoin(s, Produto.id == s.c.id_produto)
        .order_by(nullsfirst(asc(s.c.ultima_execucao)))
    )

    # 4. Se estivermos retomando um trabalho, adicionamos um filtro extra.
    if not novo_historico:
        print(f"Retomando execução interrompida do histórico ID: {historico.id}")
        
        # Subquery para pegar IDs de produtos JÁ PROCESSADOS neste histórico
        produtos_ja_processados_subquery = (
            select(Scraping.id_produto)
            .filter(Scraping.id_scraping_historico == historico.id)
            .scalar_subquery() # Usar scalar_subquery para usar com .not_in()
        )
        
        # Adiciona o filtro à query base para excluir os produtos já processados
        query_final = query_base.filter(
            Produto.id.not_in(produtos_ja_processados_subquery)
        )
    else:
        print("Iniciando nova execução de scraping.")
        # Se for uma nova execução, a query base já está correta.
        query_final = query_base

    # 5. Aplica o limite no final e executa a query.
    #    Agora o .limit() será aplicado em ambos os casos (nova execução ou retomada).
    produtos = db.scalars(query_final.limit(LIMITE_DA_BUSCA)).all()
        
    total_produtos = len(produtos)

    if total_produtos == 0:
        print("Nenhum produto novo ou antigo para processar.")
        # Se for um histórico retomado, talvez todos já tenham sido processados.
        if not novo_historico:
            historico.status = "finalizado"
            historico.fim_execucao = datetime.utcnow()
            db.commit()
        return

    # Atualiza o número de links no histórico caso seja uma nova execução.
    # Se for uma retomada, o número total já foi definido quando foi criado.
    if novo_historico:
        historico.numero_de_links = total_produtos
        db.commit()
    
    casos_erros = 0
    # O número de processados deve começar a contar a partir do que já existe no histórico
    produtos_processados_nesta_execucao = 0
    total_a_processar_nesta_execucao = len(produtos)


    try:
        # 6. Processa os produtos em blocos
        for i in range(0, total_a_processar_nesta_execucao, TAMANHO_BLOCO):
            bloco = produtos[i:i + TAMANHO_BLOCO]

            for produto in bloco:
                produtos_processados_nesta_execucao += 1
                print(f"Progresso nesta sessão: {produtos_processados_nesta_execucao}/{total_a_processar_nesta_execucao} "
                      f"({produtos_processados_nesta_execucao/total_a_processar_nesta_execucao:.1%}) - ID do Produto: {produto.id}")

                try:
                    resposta = run(produto.sku_marketplace, max_tentativas=3)

                    if resposta and resposta.get('status'):
                        salvar_scraping(
                            db=db,
                            id_historico=historico.id,
                            id_produto=produto.id,
                            resposta=resposta['response']
                        )
                    else:
                        casos_erros += 1
                        print(f"❌ Erro ao processar produto ID: {produto.id}")
                        time.sleep(random.uniform(10, 25)) 

                except Exception as e:
                    casos_erros += 1
                    print(f"❌ Erro inesperado no produto ID {produto.id}: {str(e)}")

                time.sleep(random.uniform(5, 10)) 

            # Delay entre blocos
            if i + TAMANHO_BLOCO < total_a_processar_nesta_execucao:
                delay_bloco = random.uniform(10, 25)
                print(f"\n⏱️ Fim do bloco. Aguardando {delay_bloco:.1f} segundos...")
                time.sleep(delay_bloco)
            
    except Exception as e:
        print(f"❌ Erro crítico durante o processamento: {str(e)}")
        historico.status = "interrompido"
    finally:
        # Lógica de finalização do histórico
        total_processado_no_historico = db.query(Scraping).filter(Scraping.id_scraping_historico == historico.id).count()
        historico.numero_de_links = total_processado_no_historico 
        historico.fim_execucao = datetime.utcnow()
        historico.numero_erros = (historico.numero_erros or 0) + casos_erros
        
        if total_a_processar_nesta_execucao < LIMITE_DA_BUSCA:
             historico.status = "finalizado"
        else:
             historico.status = "interrompido" 

        db.commit()
        print("\n--- Resumo da Execução ---")
        print(f"Status do Histórico ({historico.id}): {historico.status}")
        print(f"Total de produtos processados nesta sessão: {produtos_processados_nesta_execucao}")
        print(f"Total de erros nesta sessão: {casos_erros}")
        print("--------------------------\n")

if __name__ == "__main__":
    db = SessionLocal()
    try:
        processar_todos_produtos(db)
    finally:
        db.close()