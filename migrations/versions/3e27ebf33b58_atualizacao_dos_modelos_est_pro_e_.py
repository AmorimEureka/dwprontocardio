"""Atualizacao dos Modelos est_pro e estoque

Revision ID: 3e27ebf33b58
Revises: 16ab36c6fe0a
Create Date: 2025-06-29 22:46:39.827543

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = '3e27ebf33b58'
down_revision: Union[str, None] = '16ab36c6fe0a'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:

    op.drop_table('itmvto_estoque')
    op.drop_table('mvto_estoque')

    op.add_column('est_pro', sa.Column('QT_MVTO', sa.String(), nullable=True))
    op.add_column('est_pro', sa.Column('QT_MENSAL', sa.String(), nullable=True))
    op.add_column('est_pro', sa.Column('QT_DIARIO', sa.String(), nullable=True))
    op.add_column('est_pro', sa.Column('QTD_DIAS_ESTOQUE', sa.String(), nullable=True))

    op.drop_column('est_pro', 'QT_CONSUMO_MES')
    op.drop_column('est_pro', 'QT_ESTOQUE_VIRTUAL')
    op.drop_column('est_pro', 'QT_ESTOQUE_MINIMO')
    op.drop_column('est_pro', 'QT_ESTOQUE_MAXIMO')
    op.drop_column('est_pro', 'QT_PONTO_DE_PEDIDO')
    op.drop_column('est_pro', 'QT_SOLICITACAO_DE_COMPRA')
    op.drop_column('est_pro', 'QT_ESTOQUE_RESERVADO')
    op.drop_column('est_pro', 'DS_LOCALIZACAO_PRATELEIRA')
    op.drop_column('est_pro', 'QT_ESTOQUE_DOADO')
    op.drop_column('est_pro', 'QT_ORDEM_DE_COMPRA')




def downgrade() -> None:

    op.add_column('est_pro', sa.Column('QT_ORDEM_DE_COMPRA', sa.VARCHAR(), autoincrement=False, nullable=True))
    op.add_column('est_pro', sa.Column('QT_ESTOQUE_DOADO', sa.VARCHAR(), autoincrement=False, nullable=True))
    op.add_column('est_pro', sa.Column('DS_LOCALIZACAO_PRATELEIRA', sa.VARCHAR(), autoincrement=False, nullable=True))
    op.add_column('est_pro', sa.Column('QT_ESTOQUE_RESERVADO', sa.VARCHAR(), autoincrement=False, nullable=True))
    op.add_column('est_pro', sa.Column('QT_SOLICITACAO_DE_COMPRA', sa.VARCHAR(), autoincrement=False, nullable=True))
    op.add_column('est_pro', sa.Column('QT_PONTO_DE_PEDIDO', sa.VARCHAR(), autoincrement=False, nullable=True))
    op.add_column('est_pro', sa.Column('QT_ESTOQUE_MAXIMO', sa.VARCHAR(), autoincrement=False, nullable=True))
    op.add_column('est_pro', sa.Column('QT_ESTOQUE_MINIMO', sa.VARCHAR(), autoincrement=False, nullable=True))
    op.add_column('est_pro', sa.Column('QT_ESTOQUE_VIRTUAL', sa.VARCHAR(), autoincrement=False, nullable=True))
    op.add_column('est_pro', sa.Column('QT_CONSUMO_MES', sa.VARCHAR(), autoincrement=False, nullable=True))

    op.drop_column('est_pro', 'QT_DIARIO')
    op.drop_column('est_pro', 'QT_MENSAL')
    op.drop_column('est_pro', 'QT_MVTO')
    op.drop_column('est_pro', 'QTD_DIAS_ESTOQUE')
