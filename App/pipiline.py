import logging
from datetime import datetime, date
from sqlalchemy.orm import Session, aliased
from sqlalchemy import desc, select, func, nullsfirst, asc
import time
import random
from decimal import Decimal
import psycopg2

# Supondo que estes módulos existam no ambiente de execução
from models import *
from database import SessionLocal
from scraping_simplicado import run, delay

# Configuração do logging para escrever em um arquivo
# O arquivo se chamará 'scraping_vm.log' e será criado no mesmo diretório do script.
# O modo 'a' significa 'append', então os logs de execuções diferentes serão adicionados ao final do arquivo.
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='scraping_vm.log',
    filemode='a'
)

def criar_novo_historico(db: Session, numero_de_links: int):
    """
    Cria um novo registro de histórico de scraping ou retoma um interrompido.
    """
    try:
        # Verifica se já existe um histórico interrompido mais recente
        historico_interrompido = (
            db.query(ScrapingHistorico)
            .filter(ScrapingHistorico.status == "interrompido")
            .order_by(desc(ScrapingHistorico.data_scraping), desc(ScrapingHistorico.inicio_execucao))
            .first()
        )

        if historico_interrompido:
            # Retorna o histórico existente para ser retomado
            return historico_interrompido, False

        # Se não existir um interrompido, cria um novo
        novo_historico = ScrapingHistorico(
            data_scraping=date.today(),
            inicio_execucao=datetime.utcnow(),
            numero_de_links=numero_de_links,
            status="em_andamento"
        )

        db.add(novo_historico)
        db.commit()
        db.refresh(novo_historico)
        return novo_historico, True

    except Exception as e:
        logging.error(f"Erro ao criar ou buscar histórico: {e}")
        db.rollback()
        raise

def salvar_scraping(db: Session, id_historico: int, id_produto: int, resposta: dict):
    """
    Salva o resultado do scraping de um único produto no banco de dados.
    """
    try:
        scraping = Scraping(
            id_scraping_historico=id_historico,
            id_produto=id_produto,
            link_ativo=resposta.get("link_ativo", None),
            nome_produto=resposta.get("nome_produto", None),
            preco_produto=resposta.get("preco_produto", 0),
            vendedor=resposta.get("vendedor", None),
            tag_sem_estoque=resposta.get("tag_sem_estoque", False),
            tag_ultimas_unidades=resposta.get("tag_ultimas_unidades", False),
            descricao_erro=resposta.get("descricao_erro", None),
        )

        db.add(scraping)
        db.commit()
        db.refresh(scraping)

        logging.info(f"Scraping salvo com sucesso para o produto ID: {id_produto}, Registro de Scraping ID: {scraping.id}")
        return scraping
    except Exception as e:
        logging.error(f"Erro ao salvar scraping para o produto ID {id_produto}: {e}")
        db.rollback()

def processar_todos_produtos(db: Session):
    """
    Orquestra todo o processo de scraping, desde a seleção de produtos até a execução e salvamento.
    """
    logging.info("======================================================================")
    logging.info(f"INICIANDO NOVA EXECUÇÃO DO SCRIPT - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logging.info("======================================================================")
    
   
    TAMANHO_BLOCO = 30
    LIMITE_DA_BUSCA = 250

    # 1. Subconsulta para encontrar a data da última pesquisa de cada produto.
    subquery_ultimos_produtos = (
        select(
            Scraping.id_produto,
            func.max(Scraping.data_criacao).label("ultima_execucao")
        )
        .group_by(Scraping.id_produto)
        .subquery()
    )
    s = aliased(subquery_ultimos_produtos)

    # 2. Verifica se existe uma execução para ser retomada ou cria uma nova.
    historico, novo_historico = criar_novo_historico(db, LIMITE_DA_BUSCA)
    
    # 3. Constrói a query base com a lógica de ordenação (produtos menos recentes primeiro).
    query_base = (
        select(Produto)
        .outerjoin(s, Produto.id == s.c.id_produto)
        .order_by(nullsfirst(asc(s.c.ultima_execucao)))
    )

    # 4. Ajusta a query se estivermos retomando um trabalho interrompido.
    if not novo_historico:
        logging.info(f"Retomando execução interrompida do histórico ID: {historico.id}")
        
        # Subquery para pegar IDs de produtos JÁ PROCESSADOS neste histórico específico.
        produtos_ja_processados_subquery = (
            select(Scraping.id_produto)
            .filter(Scraping.id_scraping_historico == historico.id)
            .scalar_subquery()
        )
        
        # Adiciona o filtro à query base para excluir os produtos já processados.
        query_final = query_base.filter(
            Produto.id.not_in(produtos_ja_processados_subquery)
        )
    else:
        logging.info("Iniciando nova execução de scraping.")
        # Se for uma nova execução, a query base já está correta.
        query_final = query_base

    # 5. Aplica o limite e executa a query para buscar os produtos a serem processados.
    produtos = db.scalars(query_final.limit(LIMITE_DA_BUSCA)).all()
        
    total_a_processar_nesta_execucao = len(produtos)

    if total_a_processar_nesta_execucao == 0:
        logging.info("Nenhum produto novo ou pendente para processar.")
        # Se for um histórico retomado que já foi concluído, marca como finalizado.
        if not novo_historico:
            historico.status = "finalizado"
            historico.fim_execucao = datetime.utcnow()
            db.commit()
            logging.info(f"Histórico ID {historico.id} retomado e finalizado pois não haviam produtos pendentes.")
        return

    # Atualiza o número de links no histórico se for uma nova execução.
    if novo_historico:
        historico.numero_de_links = total_a_processar_nesta_execucao
        db.commit()
    
    casos_erros = 0
    produtos_processados_nesta_execucao = 0

    try:
        # 6. Processa os produtos em blocos
        for i in range(0, total_a_processar_nesta_execucao, TAMANHO_BLOCO):
            bloco = produtos[i:i + TAMANHO_BLOCO]

            for produto in bloco:
                produtos_processados_nesta_execucao += 1
                progresso_percentual = (produtos_processados_nesta_execucao / total_a_processar_nesta_execucao) * 100
                logging.info(
                    f"Progresso: {produtos_processados_nesta_execucao}/{total_a_processar_nesta_execucao} "
                    f"({progresso_percentual:.1f}%) - Processando Produto ID: {produto.id}"
                )

                try:
                    # Executa o scraping para o produto
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
                        logging.error(f"Falha no scraping do produto ID: {produto.id}. Resposta: {resposta.get('response', 'N/A')}")
                        time.sleep(random.uniform(10, 25)) 

                except Exception as e:
                    casos_erros += 1
                    logging.error(f"Erro inesperado ao processar o produto ID {produto.id}: {str(e)}", exc_info=True)

                # Pausa entre requisições para evitar bloqueios
                time.sleep(random.uniform(5, 10)) 

            # Pausa maior entre os blocos
            if i + TAMANHO_BLOCO < total_a_processar_nesta_execucao:
                delay_bloco = random.uniform(10, 25)
                logging.info(f"Fim do bloco. Aguardando {delay_bloco:.1f} segundos...")
                time.sleep(delay_bloco)
            
    except Exception as e:
        logging.critical(f"Erro crítico durante o processamento dos blocos: {str(e)}", exc_info=True)
        historico.status = "interrompido"
    finally:
        # Atualiza o registro de histórico com o status final e estatísticas
        logging.info("Finalizando a execução e atualizando o histórico.")
        
        # Conta o total de produtos efetivamente processados neste histórico
        total_processado_no_historico = db.query(Scraping).filter(Scraping.id_scraping_historico == historico.id).count()
        
        # Atualiza os dados do histórico
        historico.numero_de_links = total_processado_no_historico
        historico.fim_execucao = datetime.utcnow()
        historico.numero_erros = (historico.numero_erros or 0) + casos_erros
        
        # Decide o status final: 'finalizado' se todos os produtos da busca foram processados,
        # 'interrompido' caso contrário (indicando que há mais produtos a buscar na próxima execução).
        if total_a_processar_nesta_execucao < LIMITE_DA_BUSCA:
             historico.status = "finalizado"
        else:
             historico.status = "interrompido" 

        db.commit()
        
        logging.info("--- Resumo da Execução ---")
        logging.info(f"Histórico ID: {historico.id}")
        logging.info(f"Status Final: {historico.status}")
        logging.info(f"Total de produtos processados nesta sessão: {produtos_processados_nesta_execucao}")
        logging.info(f"Total de erros nesta sessão: {casos_erros}")
        logging.info(f"Total acumulado de produtos processados no histórico: {total_processado_no_historico}")
        logging.info("--------------------------")

if __name__ == "__main__":
    logging.info("Iniciando script de scraping.")
    db = SessionLocal()
    try:
        processar_todos_produtos(db)
        logging.info("Script de scraping finalizado.")
    except Exception as e:
        logging.critical(f"O script foi encerrado devido a um erro não tratado no fluxo principal: {e}", exc_info=True)
    finally:
        db.close()
        logging.info("Conexão com o banco de dados fechada.")
