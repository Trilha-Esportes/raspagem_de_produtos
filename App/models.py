from sqlalchemy import Column, Integer, String, Boolean, Date, DateTime, ForeignKey, DECIMAL, Text
from sqlalchemy.orm import relationship
from datetime import datetime
from database import Base

class Produto(Base):
    __tablename__ = "produtos"

    id = Column(Integer, primary_key=True, index=True)
    nome = Column(String(255))
    sku_anymarket = Column(String(50))
    sku_marketplace = Column(String(50))
    marketplace_id = Column(Integer)
    
    scrapings = relationship("Scraping", back_populates="produto")
  


class ScrapingHistorico(Base):
    __tablename__ = "scraping_historico"

    id = Column(Integer, primary_key=True, index=True)
    data_scraping = Column(Date, nullable=False, default=datetime.utcnow().date)
    inicio_execucao = Column(DateTime, default=datetime.utcnow)
    fim_execucao = Column(DateTime)
    numero_de_links = Column(Integer)
    numero_erros = Column(Integer, default=0)
    status = Column(String(20), default="em_andamento")

    scrapings = relationship("Scraping", back_populates="historico")


class Scraping(Base):
    __tablename__ = "scraping"

    id = Column(Integer, primary_key=True, index=True)
    id_scraping_historico = Column(Integer, ForeignKey("scraping_historico.id"), nullable=False)
    id_produto = Column(Integer, ForeignKey("produtos.id"), nullable=False)

    link_ativo = Column(Boolean)
    nome_produto = Column(String(255))
    preco_produto = Column(DECIMAL(10, 2))
    vendedor = Column(String(100))
    tag_sem_estoque = Column(Boolean)
    tag_ultimas_unidades = Column(Boolean)
    descricao_erro = Column(Text)
    data_criacao = Column(DateTime, default=datetime.utcnow)
      

    historico = relationship("ScrapingHistorico", back_populates="scrapings")
    produto = relationship("Produto", back_populates="scrapings")