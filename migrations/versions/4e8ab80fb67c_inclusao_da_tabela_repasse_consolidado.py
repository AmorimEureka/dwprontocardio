"""Inclusao da Tabela repasse_consolidado

Revision ID: 4e8ab80fb67c
Revises: fc18a2c7e992
Create Date: 2025-05-07 12:59:04.539227

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '4e8ab80fb67c'
down_revision: Union[str, None] = 'fc18a2c7e992'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:

    op.execute("CREATE SEQUENCE public.id_repasse_consolidado_seq")
    op.create_table('repasse_consolidado',
    sa.Column('id_repasse_consolidado', sa.BigInteger(), server_default=sa.text("nextval('public.id_repasse_consolidado_seq'::regclass)"), nullable=False),
    sa.Column('CD_PRO_FAT', sa.String(), nullable=True),
    sa.Column('CD_REG_FAT', sa.String(), nullable=True),
    sa.Column('CD_PRESTADOR_REPASSE', sa.String(), nullable=True),
    sa.Column('CD_ATI_MED', sa.String(), nullable=True),
    sa.Column('CD_LANC_FAT', sa.String(), nullable=True),
    sa.Column('CD_GRU_FAT', sa.String(), nullable=True),
    sa.Column('CD_GRU_PRO', sa.String(), nullable=True),
    sa.Column('CD_PROCEDIMENTO', sa.String(), nullable=True),
    sa.Column('DT_REPASSE_CONSOLIDADO', sa.DateTime(), nullable=True),
    sa.Column('DT_COMPETENCIA_FAT', sa.DateTime(), nullable=True),
    sa.Column('DT_COMPETENCIA_REP', sa.DateTime(), nullable=True),
    sa.Column('SN_PERTENCE_PACOTE', sa.String(), nullable=True),
    sa.Column('VL_SP', sa.String(), nullable=True),
    sa.Column('VL_ATO', sa.String(), nullable=True),
    sa.Column('VL_REPASSE', sa.String(), nullable=True),
    sa.Column('VL_TOTAL_CONTA', sa.String(), nullable=True),
    sa.Column('VL_BASE_REPASSADO', sa.String(), nullable=True),
    sa.Column('DT_EXTRACAO', sa.DateTime(), server_default=sa.text('now()'), nullable=True),
    sa.PrimaryKeyConstraint('id_repasse_consolidado')
    )



def downgrade() -> None:

    op.drop_table('repasse_consolidado')
    op.execute("DROP SEQUENCE public.id_repasse_consolidado_seq")

