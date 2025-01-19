# coding: utf-8
from sqlalchemy import BigInteger, Column, DateTime, String, text
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()



class EntPro(Base):
    __tablename__ = 'ent_pro'

    id_ent_pro = Column(BigInteger, primary_key=True, server_default=text("nextval('public.id_ent_pro_seq'::regclass)"))
    CD_ENT_PRO = Column(String)
    CD_TIP_ENT = Column(String)
    CD_ESTOQUE = Column(String)
    CD_FORNECEDOR = Column(String)
    CD_ORD_COM = Column(String)
    CD_USUARIO_RECEBIMENTO = Column(String)
    CD_ATENDIMENTO = Column(String)
    DT_EMISSAO = Column(DateTime)
    DT_ENTRADA = Column(DateTime)
    DT_RECEBIMENTO = Column(DateTime)
    HR_ENTRADA = Column(DateTime)
    VL_TOTAL = Column(String)
    NR_DOCUMENTO = Column(String)
    NR_CHAVE_ACESSO = Column(String)
    SN_AUTORIZADO = Column(String)
    DT_EXTRACAO = Column(DateTime, server_default=text("now()"))


class Especie(Base):
    __tablename__ = 'especie'

    id_especie = Column(BigInteger, primary_key=True, server_default=text("nextval('public.id_especie_seq'::regclass)"))
    CD_ESPECIE = Column(String)
    CD_ITEM_RES = Column(String)
    DS_ESPECIE = Column(String)
    DT_EXTRACAO = Column(DateTime, server_default=text("now()"))


class EstPro(Base):
    __tablename__ = 'est_pro'

    id_est_pro = Column(BigInteger, primary_key=True, server_default=text("nextval('public.id_est_pro_seq'::regclass)"))
    CD_ESTOQUE = Column(String)
    CD_PRODUTO = Column(String)
    CD_LOCALIZACAO = Column(String)
    DS_LOCALIZACAO_PRATELEIRA = Column(String)
    DT_ULTIMA_MOVIMENTACAO = Column(DateTime)
    QT_ESTOQUE_ATUAL = Column(String)
    QT_ESTOQUE_MAXIMO = Column(String)
    QT_ESTOQUE_MINIMO = Column(String)
    QT_ESTOQUE_VIRTUAL = Column(String)
    QT_PONTO_DE_PEDIDO = Column(String)
    QT_CONSUMO_MES = Column(String)
    QT_SOLICITACAO_DE_COMPRA = Column(String)
    QT_ORDEM_DE_COMPRA = Column(String)
    QT_ESTOQUE_DOADO = Column(String)
    QT_ESTOQUE_RESERVADO = Column(String)
    QT_CONSUMO_ATUAL = Column(String)
    TP_CLASSIFICACAO_ABC = Column(String)
    DT_EXTRACAO = Column(DateTime, server_default=text("now()"))


class Estoque(Base):
    __tablename__ = 'estoque'

    id_estoque = Column(BigInteger, primary_key=True, server_default=text("nextval('public.id_estoque_seq'::regclass)"))
    CD_ESTOQUE = Column(String)
    CD_SETOR = Column(String)
    DS_ESTOQUE = Column(String)
    TP_ESTOQUE = Column(String)
    DT_EXTRACAO = Column(DateTime, server_default=text("now()"))


class Fornecedor(Base):
    __tablename__ = 'fornecedor'

    id_fornecedor = Column(BigInteger, primary_key=True, server_default=text("nextval('public.id_fornecedor_seq'::regclass)"))
    CD_FORNECEDOR = Column(String)
    NM_FORNECEDOR = Column(String(255))
    NM_FANTASIA = Column(String(255))
    DT_INCLUSAO = Column(DateTime)
    NR_CGC_CPF = Column(String)
    TP_FORNECEDOR = Column(String)
    DT_EXTRACAO = Column(DateTime, server_default=text("now()"))


class ItentPro(Base):
    __tablename__ = 'itent_pro'

    id_itent_pro = Column(BigInteger, primary_key=True, server_default=text("nextval('public.id_itent_pro_seq'::regclass)"))
    CD_ITENT_PRO = Column(String)
    CD_ENT_PRO = Column(String)
    CD_PRODUTO = Column(String)
    CD_UNI_PRO = Column(String)
    CD_ATENDIMENTO = Column(String)
    CD_CUSTO_MEDIO = Column(String)
    CD_PRODUTO_FORNECEDOR = Column(String)
    DT_GRAVACAO = Column(DateTime)
    QT_ENTRADA = Column(String)
    QT_DEVOLVIDA = Column(String)
    QT_ATENDIDA = Column(String)
    VL_UNITARIO = Column(String)
    VL_CUSTO_REAL = Column(String)
    VL_TOTAL_CUSTO_REAL = Column(String)
    VL_TOTAL = Column(String)
    DT_EXTRACAO = Column(DateTime, server_default=text("now()"))


class ItmvtoEstoque(Base):
    __tablename__ = 'itmvto_estoque'

    id_itmvto_estoque = Column(BigInteger, primary_key=True, server_default=text("nextval('public.id_itmvto_estoque_seq'::regclass)"))
    CD_ITMVTO_ESTOQUE = Column(String)
    CD_MVTO_ESTOQUE = Column(String)
    CD_PRODUTO = Column(String)
    CD_UNI_PRO = Column(String)
    CD_LOTE = Column(String)
    CD_ITENT_PRO = Column(String)
    CD_FORNECEDOR = Column(String)
    CD_ITPRE_MED = Column(String)
    DT_VALIDADE = Column(DateTime)
    DH_MVTO_ESTOQUE = Column(DateTime)
    QT_MOVIMENTACAO = Column(String)
    VL_UNITARIO = Column(String)
    TP_ESTOQUE = Column(String)
    DT_EXTRACAO = Column(DateTime, server_default=text("now()"))


class ItordPro(Base):
    __tablename__ = 'itord_pro'

    id_itord_pro = Column(BigInteger, primary_key=True, server_default=text("nextval('public.id_itord_pro_seq'::regclass)"))
    CD_ORD_COM = Column(String)
    CD_PRODUTO = Column(String)
    CD_UNI_PRO = Column(String)
    CD_MOT_CANCEL = Column(String)
    DT_CANCEL = Column(DateTime)
    QT_COMPRADA = Column(String)
    QT_ATENDIDA = Column(String)
    QT_RECEBIDA = Column(String)
    QT_CANCELADA = Column(String)
    VL_UNITARIO = Column(String)
    VL_TOTAL = Column(String)
    VL_CUSTO_REAL = Column(String)
    VL_TOTAL_CUSTO_REAL = Column(String)
    DT_EXTRACAO = Column(DateTime, server_default=text("now()"))


class ItsolCom(Base):
    __tablename__ = 'itsol_com'

    id_itsol_com = Column(BigInteger, primary_key=True, server_default=text("nextval('public.id_itsol_com_seq'::regclass)"))
    CD_SOL_COM = Column(String)
    CD_PRODUTO = Column(String)
    CD_UNI_PRO = Column(String)
    CD_MOT_CANCEL = Column(String)
    DT_CANCEL = Column(DateTime)
    QT_SOLIC = Column(String)
    QT_COMPRADA = Column(String)
    QT_ATENDIDA = Column(String)
    SN_COMPRADO = Column(String)
    DT_EXTRACAO = Column(DateTime, server_default=text("now()"))


class LotPro(Base):
    __tablename__ = 'lot_pro'

    id_lot_pro = Column(BigInteger, primary_key=True, server_default=text("nextval('public.id_lot_pro_seq'::regclass)"))
    CD_LOT_PRO = Column(String)
    CD_ESTOQUE = Column(String)
    CD_PRODUTO = Column(String)
    CD_LOTE = Column(String)
    DT_VALIDADE = Column(DateTime)
    QT_ESTOQUE_ATUAL = Column(String)
    DT_EXTRACAO = Column(DateTime, server_default=text("now()"))


class MotCancel(Base):
    __tablename__ = 'mot_cancel'

    id_mot_cancel = Column(BigInteger, primary_key=True, server_default=text("nextval('public.id_mot_cancel_seq'::regclass)"))
    CD_MOT_CANCEL = Column(String)
    DS_MOT_CANCEL = Column(String)
    TP_MOT_CANCEL = Column(String)
    DT_EXTRACAO = Column(DateTime, server_default=text("now()"))


class MvtoEstoque(Base):
    __tablename__ = 'mvto_estoque'

    id_mvto_estoque = Column(BigInteger, primary_key=True, server_default=text("nextval('public.id_mvto_estoque_seq'::regclass)"))
    CD_MVTO_ESTOQUE = Column(String)
    CD_ESTOQUE = Column(String)
    CD_UNI_PRO = Column(String)
    CD_UNID_INT = Column(String)
    CD_SETOR = Column(String)
    CD_ESTOQUE_DESTINO = Column(String)
    CD_CUSTO_MEDIO = Column(String)
    CD_AVISO_CIRURGIA = Column(String)
    CD_ENT_PRO = Column(String)
    CD_USUARIO = Column(String)
    CD_FORNECEDOR = Column(String)
    CD_PRESTADOR = Column(String)
    CD_PRE_MED = Column(String)
    CD_ATENDIMENTO = Column(String)
    CD_MOT_DEV = Column(String)
    DT_MVTO_ESTOQUE = Column(DateTime)
    HR_MVTO_ESTOQUE = Column(DateTime)
    VL_TOTAL = Column(String)
    TP_MVTO_ESTOQUE = Column(String)
    NR_DOCUMENTO = Column(String)
    CHAVE_NFE = Column(String)
    DT_EXTRACAO = Column(DateTime, server_default=text("now()"))


class OrdCom(Base):
    __tablename__ = 'ord_com'

    id_ord_com = Column(BigInteger, primary_key=True, server_default=text("nextval('public.id_ord_com_seq'::regclass)"))
    CD_ORD_COM = Column(String)
    CD_ESTOQUE = Column(String)
    CD_FORNECEDOR = Column(String)
    CD_SOL_COM = Column(String)
    CD_MOT_CANCEL = Column(String)
    CD_USUARIO_CRIADOR_OC = Column(String)
    CD_ULTIMO_USU_ALT_OC = Column(String)
    DT_ORD_COM = Column(DateTime)
    DT_CANCELAMENTO = Column(DateTime)
    DT_AUTORIZACAO = Column(DateTime)
    DT_ULTIMA_ALTERACAO_OC = Column(DateTime)
    TP_SITUACAO = Column(String)
    TP_ORD_COM = Column(String)
    SN_AUTORIZADO = Column(String)
    DT_EXTRACAO = Column(DateTime, server_default=text("now()"))


class Produto(Base):
    __tablename__ = 'produto'

    id_produto = Column(BigInteger, primary_key=True, server_default=text("nextval('public.id_produto_seq'::regclass)"))
    CD_PRODUTO = Column(String)
    CD_ESPECIE = Column(String)
    DS_PRODUTO = Column(String)
    DS_PRODUTO_RESUMIDO = Column(String)
    DT_CADASTRO = Column(DateTime)
    DT_ULTIMA_ENTRADA = Column(DateTime)
    HR_ULTIMA_ENTRADA = Column(DateTime)
    QT_ESTOQUE_ATUAL = Column(String)
    QT_ULTIMA_ENTRADA = Column(String)
    VL_ULTIMA_ENTRADA = Column(String)
    VL_CUSTO_MEDIO = Column(String)
    VL_ULTIMA_CUSTO_REAL = Column(String)
    DT_EXTRACAO = Column(DateTime, server_default=text("now()"))


class Setor(Base):
    __tablename__ = 'setor'

    id_setor = Column(BigInteger, primary_key=True, server_default=text("nextval('public.id_setor_seq'::regclass)"))
    CD_SETOR = Column(String)
    CD_FATOR = Column(String)
    CD_GRUPO_DE_CUSTO = Column(String)
    CD_SETOR_CUSTO = Column(String)
    NM_SETOR = Column(String)
    SN_ATIVO = Column(String)
    DT_EXTRACAO = Column(DateTime, server_default=text("now()"))


class SolCom(Base):
    __tablename__ = 'sol_com'

    id_sol_com = Column(BigInteger, primary_key=True, server_default=text("nextval('public.id_sol_com_seq'::regclass)"))
    CD_SOL_COM = Column(String)
    CD_MOT_PED = Column(String)
    CD_SETOR = Column(String)
    CD_ESTOQUE = Column(String)
    CD_MOT_CANCEL = Column(String)
    CD_ATENDIME = Column(String)
    CD_USUARIO = Column(String)
    NM_SOLICITANTE = Column(String)
    DT_SOL_COM = Column(DateTime)
    DT_CANCELAMENTO = Column(DateTime)
    VL_TOTAL = Column(String)
    TP_SITUACAO = Column(String)
    TP_SOL_COM = Column(String)
    SN_URGENTE = Column(String)
    SN_APROVADA = Column(String)
    SN_OPME = Column(String)
    DT_EXTRACAO = Column(DateTime, server_default=text("now()"))


class UniPro(Base):
    __tablename__ = 'uni_pro'

    id_uni_pro = Column(BigInteger, primary_key=True, server_default=text("nextval('public.id_uni_pro_seq'::regclass)"))
    CD_UNI_PRO = Column(String)
    CD_UNIDADE = Column(String)
    CD_PRODUTO = Column(String)
    VL_FATOR = Column(String)
    DS_UNIDADE = Column(String)
    TP_RELATORIOS = Column(String)
    SN_ATIVO = Column(String)
    DT_EXTRACAO = Column(DateTime, server_default=text("now()"))
