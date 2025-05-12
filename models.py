# coding: utf-8
from sqlalchemy import BigInteger, Column, DateTime, String, Sequence, text
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()



class EntPro(Base):
    __tablename__ = 'ent_pro'

    id_ent_pro_seq = Sequence('id_ent_pro_seq', schema='public')
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

    id_especie_seq = Sequence('id_especie_seq', schema='public')
    id_especie = Column(BigInteger, primary_key=True, server_default=text("nextval('public.id_especie_seq'::regclass)"))
    CD_ESPECIE = Column(String)
    CD_ITEM_RES = Column(String)
    DS_ESPECIE = Column(String)
    DT_EXTRACAO = Column(DateTime, server_default=text("now()"))


class EstPro(Base):
    __tablename__ = 'est_pro'

    id_est_pro_seq = Sequence('id_est_pro_seq', schema='public')
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

    id_estoque_seq = Sequence('id_estoque_seq', schema='public')
    id_estoque = Column(BigInteger, primary_key=True, server_default=text("nextval('public.id_estoque_seq'::regclass)"))
    CD_ESTOQUE = Column(String)
    CD_SETOR = Column(String)
    DS_ESTOQUE = Column(String)
    TP_ESTOQUE = Column(String)
    DT_EXTRACAO = Column(DateTime, server_default=text("now()"))


class Fornecedor(Base):
    __tablename__ = 'fornecedor'

    id_fornecedor_seq = Sequence('id_fornecedor_seq', schema='public')
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

    id_itent_pro_seq = Sequence('id_itent_pro_seq', schema='public')
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

    id_itmvto_estoque_seq = Sequence('id_itmvto_estoque_seq', schema='public')
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

    id_itord_pro_seq = Sequence('id_itord_pro_seq', schema='public')
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

    id_itsol_com_seq = Sequence('id_itsol_com_seq', schema='public')
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

    id_lot_pro_seq = Sequence('id_lot_pro_seq', schema='public')
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

    id_mot_cancel_seq = Sequence('id_mot_cancel_seq', schema='public')
    id_mot_cancel = Column(BigInteger, primary_key=True, server_default=text("nextval('public.id_mot_cancel_seq'::regclass)"))
    CD_MOT_CANCEL = Column(String)
    DS_MOT_CANCEL = Column(String)
    TP_MOT_CANCEL = Column(String)
    DT_EXTRACAO = Column(DateTime, server_default=text("now()"))


class MvtoEstoque(Base):
    __tablename__ = 'mvto_estoque'

    id_mvto_estoque_seq = Sequence('id_mvto_estoque_seq', schema='public')
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

    id_ord_com_seq = Sequence('id_ord_com_seq', schema='public')
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

    id_produto_seq = Sequence('id_produto_seq', schema='public')
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

    id_setor_seq = Sequence('id_setor_seq', schema='public')
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

    id_sol_com_seq = Sequence('id_sol_com_seq', schema='public')
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

    id_uni_pro_seq = Sequence('id_uni_pro_seq', schema='public')
    id_uni_pro = Column(BigInteger, primary_key=True, server_default=text("nextval('public.id_uni_pro_seq'::regclass)"))
    CD_UNI_PRO = Column(String)
    CD_UNIDADE = Column(String)
    CD_PRODUTO = Column(String)
    VL_FATOR = Column(String)
    DS_UNIDADE = Column(String)
    TP_RELATORIOS = Column(String)
    SN_ATIVO = Column(String)
    DT_EXTRACAO = Column(DateTime, server_default=text("now()"))

class Atendime(Base):
    __tablename__ = 'atendime'

    id_atendime_seq = Sequence('id_atendime_seq', schema='public')
    id_atendime = Column(BigInteger, primary_key=True, server_default=text("nextval('public.id_atendime_seq'::regclass)"))
    CD_PACIENTE = Column(String)
    CD_PRESTADOR = Column(String)
    DT_ATENDIMENTO = Column(DateTime)
    DT_EXTRACAO = Column(DateTime, server_default=text("now()"))


class Ati_Med(Base):
    __tablename__ = 'ati_med'

    id_ati_med_seq = Sequence('id_ati_med_seq', schema='public')
    id_ati_med = Column(BigInteger, primary_key=True, server_default=text("nextval('public.id_ati_med_seq'::regclass)"))
    CD_ATENDIMENTO = Column(String)
    CD_ATI_MED = Column(String)
    DS_ATI_MED = Column(String)
    DT_EXTRACAO = Column(DateTime, server_default=text("now()"))


class Convenio(Base):
    __tablename__ = 'convenio'

    id_convenio_seq = Sequence('id_convenio_seq', schema='public')
    id_convenio = Column(BigInteger, primary_key=True, server_default=text("nextval('public.id_convenio_seq'::regclass)"))
    CD_CONVENIO = Column(String)
    NM_CONVENIO = Column(String)
    DT_EXTRACAO = Column(DateTime, server_default=text("now()"))


class Gru_Fat(Base):
    __tablename__ = 'gru_fat'

    id_gru_fat_seq = Sequence('id_gru_fat_seq', schema='public')
    id_gru_fat = Column(BigInteger, primary_key=True, server_default=text("nextval('public.id_gru_fat_seq'::regclass)"))
    CD_GRU_FAT = Column(String)
    DS_GRU_FAT = Column(String)
    DT_EXTRACAO = Column(DateTime, server_default=text("now()"))


class Gru_Pro(Base):
    __tablename__ = 'gru_pro'

    id_gru_pro_seq = Sequence('id_gru_pro_seq', schema='public')
    id_gru_pro = Column(BigInteger, primary_key=True, server_default=text("nextval('public.id_gru_pro_seq'::regclass)"))
    CD_GRU_PRO = Column(String)
    DS_GRU_PRO = Column(String)
    DT_EXTRACAO = Column(DateTime, server_default=text("now()"))


class It_Repasse_SIH(Base):
    __tablename__ = 'it_repasse_sih'

    it_repasse_sih_seq = Sequence('it_repasse_sih_seq', schema='public')
    id_it_repasse_sih = Column(BigInteger, primary_key=True, server_default=text("nextval('public.id_it_repasse_sih_seq'::regclass)"))
    CD_REPASSE = Column(String)
    CD_REG_FAT = Column(String)
    CD_LANCAMENTO = Column(String)
    CD_ATI_MED = Column(String)
    CD_PRESTADOR_REPASSE = Column(String)
    VL_REPASSE = Column(String)
    DT_EXTRACAO = Column(DateTime, server_default=text("now()"))


class It_Repasse(Base):
    __tablename__ = 'it_repasse'

    it_repasse_seq = Sequence('it_repasse_seq', schema='public')
    id_it_repasse = Column(BigInteger, primary_key=True, server_default=text("nextval('public.id_it_repasse_seq'::regclass)"))
    CD_REPASSE = Column(String)
    CD_REG_AMB = Column(String)
    CD_LANCAMENTO_AMB = Column(String)
    CD_REG_FAT = Column(String)
    CD_LANCAMENTO_FAT = Column(String)
    CD_ATI_MED = Column(String)
    CD_PRESTADOR_REPASSE = Column(String)
    VL_REPASSE = Column(String)
    DT_EXTRACAO = Column(DateTime, server_default=text("now()"))


class Itreg_Amb(Base):
    __tablename__ = 'itreg_amb'

    itreg_amb_seq = Sequence('itreg_amb_seq', schema='public')
    id_itreg_amb = Column(BigInteger, primary_key=True, server_default=text("nextval('public.id_itreg_amb_seq'::regclass)"))
    CD_PRO_FAT = Column(String)
    CD_REG_AMB = Column(String)
    CD_PRESTADOR = Column(String)
    CD_ATI_MED = Column(String)
    CD_LANCAMENTO = Column(String)
    CD_GRU_FAT = Column(String)
    CD_CONVENIO = Column(String)
    CD_ATENDIMENTO = Column(String)
    DT_PRODUCAO = Column(DateTime)
    DT_FECHAMENTO = Column(DateTime)
    DT_ITREG_AMB = Column(DateTime)
    SN_FECHADA = Column(String)
    SN_REPASSADO = Column(String)
    SN_PERTENCE_PACOTE = Column(String)
    VL_UNITARIO = Column(String)
    VL_TOTAL_CONTA = Column(String)
    VL_BASE_REPASSADO = Column(String)
    DT_EXTRACAO = Column(DateTime, server_default=text("now()"))


class Itreg_Fat(Base):
    __tablename__ = 'itreg_fat'

    itreg_fat_seq = Sequence('itreg_fat_seq', schema='public')
    id_itreg_fat = Column(BigInteger, primary_key=True, server_default=text("nextval('public.id_itreg_fat_seq'::regclass)"))
    CD_PRO_FAT = Column(String)
    CD_REG_FAT = Column(String)
    CD_PRESTADOR = Column(String)
    CD_ATI_MED = Column(String)
    CD_LANCAMENTO = Column(String)
    CD_GRU_FAT = Column(String)
    CD_PROCEDIMENTO = Column(String)
    DT_PRODUCAO = Column(DateTime)
    DT_ITREG_FAT = Column(DateTime)
    SN_REPASSADO = Column(String)
    SN_PERTENCE_PACOTE = Column(String)
    VL_UNITARIO = Column(String)
    VL_SP = Column(String)
    VL_ATO = Column(String)
    VL_TOTAL_CONTA = Column(String)
    VL_BASE_REPASSADO = Column(String)
    DT_EXTRACAO = Column(DateTime, server_default=text("now()"))


class Paciente(Base):
    __tablename__ = 'paciente'

    id_paciente_seq = Sequence('id_paciente_seq', schema='public')
    id_paciente = Column(BigInteger, primary_key=True, server_default=text("nextval('public.id_paciente_seq'::regclass)"))
    CD_PACIENTE = Column(String)
    NM_PACIENTE = Column(String)
    DT_EXTRACAO = Column(DateTime, server_default=text("now()"))


class Prestador(Base):
    __tablename__ = 'prestador'

    id_prestador_seq = Sequence('id_prestador_seq', schema='public')
    id_prestador = Column(BigInteger, primary_key=True, server_default=text("nextval('public.id_prestador_seq'::regclass)"))
    CD_PRESTADOR = Column(String)
    NM_PRESTADOR = Column(String)
    DT_EXTRACAO = Column(DateTime, server_default=text("now()"))


class Pro_Fat(Base):
    __tablename__ = 'pro_fat'

    id_pro_fat_seq = Sequence('id_pro_fat_seq', schema='public')
    id_pro_fat = Column(BigInteger, primary_key=True, server_default=text("nextval('public.id_pro_fat_seq'::regclass)"))
    CD_PRO_FAT = Column(String)
    CD_GRU_PRO = Column(String)
    DS_GRU_PRO = Column(String)
    DT_EXTRACAO = Column(DateTime, server_default=text("now()"))


class Reg_Amb(Base):
    __tablename__ = 'reg_amb'

    id_reg_amb_seq = Sequence('id_reg_amb_seq', schema='public')
    id_reg_amb = Column(BigInteger, primary_key=True, server_default=text("nextval('public.id_reg_amb_seq'::regclass)"))
    CD_REG_AMB = Column(String)
    CD_REMESSA = Column(String)
    DT_REG_AMB = Column(DateTime)
    DT_REMESSA = Column(DateTime)
    DT_EXTRACAO = Column(DateTime, server_default=text("now()"))


class Reg_Fat(Base):
    __tablename__ = 'reg_fat'

    id_reg_fat_seq = Sequence('id_reg_fat_seq', schema='public')
    id_reg_fat = Column(BigInteger, primary_key=True, server_default=text("nextval('public.id_reg_fat_seq'::regclass)"))
    CD_REG_FAT = Column(String)
    CD_CONVENIO = Column(String)
    CD_ATENDIMENTO = Column(String)
    CD_REMESSA = Column(String)
    DT_REG_FAT = Column(DateTime)
    DT_REMESSA = Column(DateTime)
    DT_EXTRACAO = Column(DateTime, server_default=text("now()"))


class Repasse(Base):
    __tablename__ = 'repasse'

    id_repasse_seq = Sequence('id_repasse_seq', schema='public')
    id_repasse = Column(BigInteger, primary_key=True, server_default=text("nextval('public.id_repasse_seq'::regclass)"))
    CD_REPASSE = Column(String)
    TP_REPASSE = Column(String)
    DT_COMPETENCIA = Column(DateTime)
    DT_REPASSE = Column(DateTime)
    DT_EXTRACAO = Column(DateTime, server_default=text("now()"))

class Repasse_Prestador(Base):
    __tablename__ = 'repasse_prestador'

    id_repasse_prestador_seq = Sequence('id_repasse_prestador_seq', schema='public')
    id_repasse_prestador = Column(BigInteger, primary_key=True, server_default=text("nextval('public.id_repasse_prestador_seq'::regclass)"))
    CD_REPASSE = Column(String)
    CD_PRESTADOR_REPASSE = Column(String)
    DS_REPASSE = Column(String)
    DT_COMPETENCIA = Column(DateTime)
    DT_REPASSE = Column(DateTime)
    TP_REPASSE = Column(String)
    VL_REPASSE = Column(String)
    DT_EXTRACAO = Column(DateTime, server_default=text("now()"))

class Procedimento_Sus(Base):
    __tablename__ = 'procedimento_sus'

    id_procedimento_sus_seq = Sequence('id_procedimento_sus_seq', schema='public')
    id_procedimento_sus = Column(BigInteger, primary_key=True, server_default=text("nextval('public.id_procedimento_sus_seq'::regclass)"))
    CD_PROCEDIMENTO = Column(String)
    DS_PROCEDIMENTO = Column(String)
    DT_EXTRACAO = Column(DateTime, server_default=text("now()"))

class repasse_consolidado(Base):
    __tablename__ = 'repasse_consolidado'

    id_repasse_consolidado_seq = Sequence('id_repasse_consolidado_seq', schema='public')
    id_repasse_consolidado = Column(BigInteger, primary_key=True, server_default=text("nextval('public.id_repasse_consolidado_seq'::regclass)"))
    CD_PRO_FAT = Column(String)
    CD_REG_FAT = Column(String)
    CD_PRESTADOR_REPASSE = Column(String)
    CD_ATI_MED = Column(String)
    CD_LANC_FAT = Column(String)
    CD_GRU_FAT = Column(String)
    CD_GRU_PRO = Column(String)
    CD_PROCEDIMENTO = Column(String)
    DT_REPASSE_CONSOLIDADO  = Column(DateTime)
    DT_COMPETENCIA_FAT = Column(DateTime)
    DT_COMPETENCIA_REP = Column(DateTime)
    SN_PERTENCE_PACOTE = Column(String)
    VL_SP = Column(String)
    VL_ATO = Column(String)
    VL_REPASSE = Column(String)
    VL_TOTAL_CONTA = Column(String)
    VL_BASE_REPASSADO = Column(String)
    DT_EXTRACAO = Column(DateTime, server_default=text("now()"))