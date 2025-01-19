"""Criando tabelas do schema raw_mv

Revision ID: 67783182a321
Revises: 
Create Date: 2025-01-18 20:47:58.632978

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa



revision: str = '67783182a321'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None



def upgrade() -> None:

    op.create_table(
        'ent_pro',
        sa.Column('id_ent_pro', sa.BigInteger(), server_default=sa.text("nextval('public.id_ent_pro_seq'::regclass)"), nullable=False),
        sa.Column('CD_ENT_PRO', sa.String(), nullable=True),
        sa.Column('CD_TIP_ENT', sa.String(), nullable=True),
        sa.Column('CD_ESTOQUE', sa.String(), nullable=True),
        sa.Column('CD_FORNECEDOR', sa.String(), nullable=True),
        sa.Column('CD_ORD_COM', sa.String(), nullable=True),
        sa.Column('CD_USUARIO_RECEBIMENTO', sa.String(), nullable=True),
        sa.Column('CD_ATENDIMENTO', sa.String(), nullable=True),
        sa.Column('DT_EMISSAO', sa.DateTime(), nullable=True),
        sa.Column('DT_ENTRADA', sa.DateTime(), nullable=True),
        sa.Column('DT_RECEBIMENTO', sa.DateTime(), nullable=True),
        sa.Column('HR_ENTRADA', sa.DateTime(), nullable=True),
        sa.Column('VL_TOTAL', sa.String(), nullable=True),
        sa.Column('NR_DOCUMENTO', sa.String(), nullable=True),
        sa.Column('NR_CHAVE_ACESSO', sa.String(), nullable=True),
        sa.Column('SN_AUTORIZADO', sa.String(), nullable=True),
        sa.Column('DT_EXTRACAO', sa.DateTime(), server_default=sa.text('now()'), nullable=True),
        sa.PrimaryKeyConstraint('id_ent_pro')
    )


    op.create_table(
        'especie',
        sa.Column('id_especie', sa.BigInteger(), server_default=sa.text("nextval('public.id_especie_seq'::regclass)"), nullable=False),
        sa.Column('CD_ESPECIE', sa.String(), nullable=True),
        sa.Column('CD_ITEM_RES', sa.String(), nullable=True),
        sa.Column('DS_ESPECIE', sa.String(), nullable=True),
        sa.Column('DT_EXTRACAO', sa.DateTime(), server_default=sa.text('now()'), nullable=True),
        sa.PrimaryKeyConstraint('id_especie')
    )


    op.create_table(
        'est_pro',
        sa.Column('id_est_pro', sa.BigInteger(), server_default=sa.text("nextval('public.id_est_pro_seq'::regclass)"), nullable=False),
        sa.Column('CD_ESTOQUE', sa.String(), nullable=True),
        sa.Column('CD_PRODUTO', sa.String(), nullable=True),
        sa.Column('CD_LOCALIZACAO', sa.String(), nullable=True),
        sa.Column('DS_LOCALIZACAO_PRATELEIRA', sa.String(), nullable=True),
        sa.Column('DT_ULTIMA_MOVIMENTACAO', sa.DateTime(), nullable=True),
        sa.Column('QT_ESTOQUE_ATUAL', sa.String(), nullable=True),
        sa.Column('QT_ESTOQUE_MAXIMO', sa.String(), nullable=True),
        sa.Column('QT_ESTOQUE_MINIMO', sa.String(), nullable=True),
        sa.Column('QT_ESTOQUE_VIRTUAL', sa.String(), nullable=True),
        sa.Column('QT_PONTO_DE_PEDIDO', sa.String(), nullable=True),
        sa.Column('QT_CONSUMO_MES', sa.String(), nullable=True),
        sa.Column('QT_SOLICITACAO_DE_COMPRA', sa.String(), nullable=True),
        sa.Column('QT_ORDEM_DE_COMPRA', sa.String(), nullable=True),
        sa.Column('QT_ESTOQUE_DOADO', sa.String(), nullable=True),
        sa.Column('QT_ESTOQUE_RESERVADO', sa.String(), nullable=True),
        sa.Column('QT_CONSUMO_ATUAL', sa.String(), nullable=True),
        sa.Column('TP_CLASSIFICACAO_ABC', sa.String(), nullable=True),
        sa.Column('DT_EXTRACAO', sa.DateTime(), server_default=sa.text('now()'), nullable=True),
        sa.PrimaryKeyConstraint('id_est_pro')
    )


    op.create_table(
        'estoque',
        sa.Column('id_estoque', sa.BigInteger(), server_default=sa.text("nextval('public.id_estoque_seq'::regclass)"), nullable=False),
        sa.Column('CD_ESTOQUE', sa.String(), nullable=True),
        sa.Column('CD_SETOR', sa.String(), nullable=True),
        sa.Column('DS_ESTOQUE', sa.String(), nullable=True),
        sa.Column('TP_ESTOQUE', sa.String(), nullable=True),
        sa.Column('DT_EXTRACAO', sa.DateTime(), server_default=sa.text('now()'), nullable=True),
        sa.PrimaryKeyConstraint('id_estoque')
    )


    op.create_table(
        'fornecedor',
        sa.Column('id_fornecedor', sa.BigInteger(), server_default=sa.text("nextval('public.id_fornecedor_seq'::regclass)"), nullable=False),
        sa.Column('CD_FORNECEDOR', sa.String(), nullable=True),
        sa.Column('NM_FORNECEDOR', sa.String(length=255), nullable=True),
        sa.Column('NM_FANTASIA', sa.String(length=255), nullable=True),
        sa.Column('DT_INCLUSAO', sa.DateTime(), nullable=True),
        sa.Column('NR_CGC_CPF', sa.String(), nullable=True),
        sa.Column('TP_FORNECEDOR', sa.String(), nullable=True),
        sa.Column('DT_EXTRACAO', sa.DateTime(), server_default=sa.text('now()'), nullable=True),
        sa.PrimaryKeyConstraint('id_fornecedor')
    )


    op.create_table(
        'itent_pro',
        sa.Column('id_itent_pro', sa.BigInteger(), server_default=sa.text("nextval('public.id_itent_pro_seq'::regclass)"), nullable=False),
        sa.Column('CD_ITENT_PRO', sa.String(), nullable=True),
        sa.Column('CD_ENT_PRO', sa.String(), nullable=True),
        sa.Column('CD_PRODUTO', sa.String(), nullable=True),
        sa.Column('CD_UNI_PRO', sa.String(), nullable=True),
        sa.Column('CD_ATENDIMENTO', sa.String(), nullable=True),
        sa.Column('CD_CUSTO_MEDIO', sa.String(), nullable=True),
        sa.Column('CD_PRODUTO_FORNECEDOR', sa.String(), nullable=True),
        sa.Column('DT_GRAVACAO', sa.DateTime(), nullable=True),
        sa.Column('QT_ENTRADA', sa.String(), nullable=True),
        sa.Column('QT_DEVOLVIDA', sa.String(), nullable=True),
        sa.Column('QT_ATENDIDA', sa.String(), nullable=True),
        sa.Column('VL_UNITARIO', sa.String(), nullable=True),
        sa.Column('VL_CUSTO_REAL', sa.String(), nullable=True),
        sa.Column('VL_TOTAL_CUSTO_REAL', sa.String(), nullable=True),
        sa.Column('VL_TOTAL', sa.String(), nullable=True),
        sa.Column('DT_EXTRACAO', sa.DateTime(), server_default=sa.text('now()'), nullable=True),
        sa.PrimaryKeyConstraint('id_itent_pro')
    )


    op.create_table(
        'itmvto_estoque',
        sa.Column('id_itmvto_estoque', sa.BigInteger(), server_default=sa.text("nextval('public.id_itmvto_estoque_seq'::regclass)"), nullable=False),
        sa.Column('CD_ITMVTO_ESTOQUE', sa.String(), nullable=True),
        sa.Column('CD_MVTO_ESTOQUE', sa.String(), nullable=True),
        sa.Column('CD_PRODUTO', sa.String(), nullable=True),
        sa.Column('CD_UNI_PRO', sa.String(), nullable=True),
        sa.Column('CD_LOTE', sa.String(), nullable=True),
        sa.Column('CD_ITENT_PRO', sa.String(), nullable=True),
        sa.Column('CD_FORNECEDOR', sa.String(), nullable=True),
        sa.Column('CD_ITPRE_MED', sa.String(), nullable=True),
        sa.Column('DT_VALIDADE', sa.DateTime(), nullable=True),
        sa.Column('DH_MVTO_ESTOQUE', sa.DateTime(), nullable=True),
        sa.Column('QT_MOVIMENTACAO', sa.String(), nullable=True),
        sa.Column('VL_UNITARIO', sa.String(), nullable=True),
        sa.Column('TP_ESTOQUE', sa.String(), nullable=True),
        sa.Column('DT_EXTRACAO', sa.DateTime(), server_default=sa.text('now()'), nullable=True),
        sa.PrimaryKeyConstraint('id_itmvto_estoque')
    )


    op.create_table(
        'itord_pro',
        sa.Column('id_itord_pro', sa.BigInteger(), server_default=sa.text("nextval('public.id_itord_pro_seq'::regclass)"), nullable=False),
        sa.Column('CD_ORD_COM', sa.String(), nullable=True),
        sa.Column('CD_PRODUTO', sa.String(), nullable=True),
        sa.Column('CD_UNI_PRO', sa.String(), nullable=True),
        sa.Column('CD_MOT_CANCEL', sa.String(), nullable=True),
        sa.Column('DT_CANCEL', sa.DateTime(), nullable=True),
        sa.Column('QT_COMPRADA', sa.String(), nullable=True),
        sa.Column('QT_ATENDIDA', sa.String(), nullable=True),
        sa.Column('QT_RECEBIDA', sa.String(), nullable=True),
        sa.Column('QT_CANCELADA', sa.String(), nullable=True),
        sa.Column('VL_UNITARIO', sa.String(), nullable=True),
        sa.Column('VL_TOTAL', sa.String(), nullable=True),
        sa.Column('VL_CUSTO_REAL', sa.String(), nullable=True),
        sa.Column('VL_TOTAL_CUSTO_REAL', sa.String(), nullable=True),
        sa.Column('DT_EXTRACAO', sa.DateTime(), server_default=sa.text('now()'), nullable=True),
        sa.PrimaryKeyConstraint('id_itord_pro')
    )


    op.create_table(
        'itsol_com',
        sa.Column('id_itsol_com', sa.BigInteger(), server_default=sa.text("nextval('public.id_itsol_com_seq'::regclass)"), nullable=False),
        sa.Column('CD_SOL_COM', sa.String(), nullable=True),
        sa.Column('CD_PRODUTO', sa.String(), nullable=True),
        sa.Column('CD_UNI_PRO', sa.String(), nullable=True),
        sa.Column('CD_MOT_CANCEL', sa.String(), nullable=True),
        sa.Column('DT_CANCEL', sa.DateTime(), nullable=True),
        sa.Column('QT_SOLIC', sa.String(), nullable=True),
        sa.Column('QT_COMPRADA', sa.String(), nullable=True),
        sa.Column('QT_ATENDIDA', sa.String(), nullable=True),
        sa.Column('SN_COMPRADO', sa.String(), nullable=True),
        sa.Column('DT_EXTRACAO', sa.DateTime(), server_default=sa.text('now()'), nullable=True),
        sa.PrimaryKeyConstraint('id_itsol_com')
    )


    op.create_table(
        'lot_pro',
        sa.Column('id_lot_pro', sa.BigInteger(), server_default=sa.text("nextval('public.id_lot_pro_seq'::regclass)"), nullable=False),
        sa.Column('CD_LOT_PRO', sa.String(), nullable=True),
        sa.Column('CD_ESTOQUE', sa.String(), nullable=True),
        sa.Column('CD_PRODUTO', sa.String(), nullable=True),
        sa.Column('CD_LOTE', sa.String(), nullable=True),
        sa.Column('DT_VALIDADE', sa.DateTime(), nullable=True),
        sa.Column('QT_ESTOQUE_ATUAL', sa.String(), nullable=True),
        sa.Column('DT_EXTRACAO', sa.DateTime(), server_default=sa.text('now()'), nullable=True),
        sa.PrimaryKeyConstraint('id_lot_pro')
    )


    op.create_table(
        'mot_cancel',
        sa.Column('id_mot_cancel', sa.BigInteger(), server_default=sa.text("nextval('public.id_mot_cancel_seq'::regclass)"), nullable=False),
        sa.Column('CD_MOT_CANCEL', sa.String(), nullable=True),
        sa.Column('DS_MOT_CANCEL', sa.String(), nullable=True),
        sa.Column('TP_MOT_CANCEL', sa.String(), nullable=True),
        sa.Column('DT_EXTRACAO', sa.DateTime(), server_default=sa.text('now()'), nullable=True),
        sa.PrimaryKeyConstraint('id_mot_cancel')
    )


    op.create_table(
        'mvto_estoque',
        sa.Column('id_mvto_estoque', sa.BigInteger(), server_default=sa.text("nextval('public.id_mvto_estoque_seq'::regclass)"), nullable=False),
        sa.Column('CD_MVTO_ESTOQUE', sa.String(), nullable=True),
        sa.Column('CD_ESTOQUE', sa.String(), nullable=True),
        sa.Column('CD_UNI_PRO', sa.String(), nullable=True),
        sa.Column('CD_UNID_INT', sa.String(), nullable=True),
        sa.Column('CD_SETOR', sa.String(), nullable=True),
        sa.Column('CD_ESTOQUE_DESTINO', sa.String(), nullable=True),
        sa.Column('CD_CUSTO_MEDIO', sa.String(), nullable=True),
        sa.Column('CD_AVISO_CIRURGIA', sa.String(), nullable=True),
        sa.Column('CD_ENT_PRO', sa.String(), nullable=True),
        sa.Column('CD_USUARIO', sa.String(), nullable=True),
        sa.Column('CD_FORNECEDOR', sa.String(), nullable=True),
        sa.Column('CD_PRESTADOR', sa.String(), nullable=True),
        sa.Column('CD_PRE_MED', sa.String(), nullable=True),
        sa.Column('CD_ATENDIMENTO', sa.String(), nullable=True),
        sa.Column('CD_MOT_DEV', sa.String(), nullable=True),
        sa.Column('DT_MVTO_ESTOQUE', sa.DateTime(), nullable=True),
        sa.Column('HR_MVTO_ESTOQUE', sa.DateTime(), nullable=True),
        sa.Column('VL_TOTAL', sa.String(), nullable=True),
        sa.Column('TP_MVTO_ESTOQUE', sa.String(), nullable=True),
        sa.Column('NR_DOCUMENTO', sa.String(), nullable=True),
        sa.Column('CHAVE_NFE', sa.String(), nullable=True),
        sa.Column('DT_EXTRACAO', sa.DateTime(), server_default=sa.text('now()'), nullable=True),
        sa.PrimaryKeyConstraint('id_mvto_estoque')
    )


    op.create_table(
        'ord_com',
        sa.Column('id_ord_com', sa.BigInteger(), server_default=sa.text("nextval('public.id_ord_com_seq'::regclass)"), nullable=False),
        sa.Column('CD_ORD_COM', sa.String(), nullable=True),
        sa.Column('CD_ESTOQUE', sa.String(), nullable=True),
        sa.Column('CD_FORNECEDOR', sa.String(), nullable=True),
        sa.Column('CD_SOL_COM', sa.String(), nullable=True),
        sa.Column('CD_MOT_CANCEL', sa.String(), nullable=True),
        sa.Column('CD_USUARIO_CRIADOR_OC', sa.String(), nullable=True),
        sa.Column('CD_ULTIMO_USU_ALT_OC', sa.String(), nullable=True),
        sa.Column('DT_ORD_COM', sa.DateTime(), nullable=True),
        sa.Column('DT_CANCELAMENTO', sa.DateTime(), nullable=True),
        sa.Column('DT_AUTORIZACAO', sa.DateTime(), nullable=True),
        sa.Column('DT_ULTIMA_ALTERACAO_OC', sa.DateTime(), nullable=True),
        sa.Column('TP_SITUACAO', sa.String(), nullable=True),
        sa.Column('TP_ORD_COM', sa.String(), nullable=True),
        sa.Column('SN_AUTORIZADO', sa.String(), nullable=True),
        sa.Column('DT_EXTRACAO', sa.DateTime(), server_default=sa.text('now()'), nullable=True),
        sa.PrimaryKeyConstraint('id_ord_com')
    )


    op.create_table(
        'produto',
        sa.Column('id_produto', sa.BigInteger(), server_default=sa.text("nextval('public.id_produto_seq'::regclass)"), nullable=False),
        sa.Column('CD_PRODUTO', sa.String(), nullable=True),
        sa.Column('CD_ESPECIE', sa.String(), nullable=True),
        sa.Column('DS_PRODUTO', sa.String(), nullable=True),
        sa.Column('DS_PRODUTO_RESUMIDO', sa.String(), nullable=True),
        sa.Column('DT_CADASTRO', sa.DateTime(), nullable=True),
        sa.Column('DT_ULTIMA_ENTRADA', sa.DateTime(), nullable=True),
        sa.Column('HR_ULTIMA_ENTRADA', sa.DateTime(), nullable=True),
        sa.Column('QT_ESTOQUE_ATUAL', sa.String(), nullable=True),
        sa.Column('QT_ULTIMA_ENTRADA', sa.String(), nullable=True),
        sa.Column('VL_ULTIMA_ENTRADA', sa.String(), nullable=True),
        sa.Column('VL_CUSTO_MEDIO', sa.String(), nullable=True),
        sa.Column('VL_ULTIMA_CUSTO_REAL', sa.String(), nullable=True),
        sa.Column('DT_EXTRACAO', sa.DateTime(), server_default=sa.text('now()'), nullable=True),
        sa.PrimaryKeyConstraint('id_produto')
    )


    op.create_table(
        'setor',
        sa.Column('id_setor', sa.BigInteger(), server_default=sa.text("nextval('public.id_setor_seq'::regclass)"), nullable=False),
        sa.Column('CD_SETOR', sa.String(), nullable=True),
        sa.Column('CD_FATOR', sa.String(), nullable=True),
        sa.Column('CD_GRUPO_DE_CUSTO', sa.String(), nullable=True),
        sa.Column('CD_SETOR_CUSTO', sa.String(), nullable=True),
        sa.Column('NM_SETOR', sa.String(), nullable=True),
        sa.Column('SN_ATIVO', sa.String(), nullable=True),
        sa.Column('DT_EXTRACAO', sa.DateTime(), server_default=sa.text('now()'), nullable=True),
        sa.PrimaryKeyConstraint('id_setor')
    )


    op.create_table(
        'sol_com',
        sa.Column('id_sol_com', sa.BigInteger(), server_default=sa.text("nextval('public.id_sol_com_seq'::regclass)"), nullable=False),
        sa.Column('CD_SOL_COM', sa.String(), nullable=True),
        sa.Column('CD_MOT_PED', sa.String(), nullable=True),
        sa.Column('CD_SETOR', sa.String(), nullable=True),
        sa.Column('CD_ESTOQUE', sa.String(), nullable=True),
        sa.Column('CD_MOT_CANCEL', sa.String(), nullable=True),
        sa.Column('CD_ATENDIME', sa.String(), nullable=True),
        sa.Column('CD_USUARIO', sa.String(), nullable=True),
        sa.Column('NM_SOLICITANTE', sa.String(), nullable=True),
        sa.Column('DT_SOL_COM', sa.DateTime(), nullable=True),
        sa.Column('DT_CANCELAMENTO', sa.DateTime(), nullable=True),
        sa.Column('VL_TOTAL', sa.String(), nullable=True),
        sa.Column('TP_SITUACAO', sa.String(), nullable=True),
        sa.Column('TP_SOL_COM', sa.String(), nullable=True),
        sa.Column('SN_URGENTE', sa.String(), nullable=True),
        sa.Column('SN_APROVADA', sa.String(), nullable=True),
        sa.Column('SN_OPME', sa.String(), nullable=True),
        sa.Column('DT_EXTRACAO', sa.DateTime(), server_default=sa.text('now()'), nullable=True),
        sa.PrimaryKeyConstraint('id_sol_com')
    )


    op.create_table(
        'uni_pro',
        sa.Column('id_uni_pro', sa.BigInteger(), server_default=sa.text("nextval('public.id_uni_pro_seq'::regclass)"), nullable=False),
        sa.Column('CD_UNI_PRO', sa.String(), nullable=True),
        sa.Column('CD_UNIDADE', sa.String(), nullable=True),
        sa.Column('CD_PRODUTO', sa.String(), nullable=True),
        sa.Column('VL_FATOR', sa.String(), nullable=True),
        sa.Column('DS_UNIDADE', sa.String(), nullable=True),
        sa.Column('TP_RELATORIOS', sa.String(), nullable=True),
        sa.Column('SN_ATIVO', sa.String(), nullable=True),
        sa.Column('DT_EXTRACAO', sa.DateTime(), server_default=sa.text('now()'), nullable=True),
        sa.PrimaryKeyConstraint('id_uni_pro')
    )



def downgrade() -> None:
    
    op.drop_table('uni_pro')
    op.drop_table('sol_com')
    op.drop_table('setor')
    op.drop_table('produto')
    op.drop_table('ord_com')
    op.drop_table('mvto_estoque')
    op.drop_table('mot_cancel')
    op.drop_table('lot_pro')
    op.drop_table('itsol_com')
    op.drop_table('itord_pro')
    op.drop_table('itmvto_estoque')
    op.drop_table('itent_pro')
    op.drop_table('fornecedor')
    op.drop_table('estoque')
    op.drop_table('est_pro')
    op.drop_table('especie')
    op.drop_table('ent_pro')
