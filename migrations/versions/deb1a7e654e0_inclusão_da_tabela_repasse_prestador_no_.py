"""Inclusão da Tabela Repasse_Prestador no Modelo Repasses Medicos

Revision ID: deb1a7e654e0
Revises: edaabd0bb8ee
Create Date: 2025-03-12 17:01:52.746544

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'deb1a7e654e0'
down_revision: Union[str, None] = 'edaabd0bb8ee'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:

    op.execute("CREATE SEQUENCE public.id_repasse_prestador_seq")
    op.create_table('repasse_prestador',
        sa.Column('id_repasse_prestador', sa.BigInteger(), server_default=sa.text("nextval('public.id_repasse_prestador_seq'::regclass)"), nullable=False),
        sa.Column('CD_REPASSE', sa.String(), nullable=True),
        sa.Column('CD_PRESTADOR_REPASSE', sa.String(), nullable=True),
        sa.Column('DS_REPASSE', sa.String(), nullable=True),
        sa.Column('DT_COMPETENCIA', sa.DateTime(), nullable=True),
        sa.Column('DT_REPASSE', sa.DateTime(), nullable=True),
        sa.Column('TP_REPASSE', sa.String(), nullable=True),
        sa.Column('VL_REPASSE', sa.String(), nullable=True),
        sa.Column('DT_EXTRACAO', sa.DateTime(), server_default=sa.text('now()'), nullable=True),
        sa.PrimaryKeyConstraint('id_repasse_prestador')
    )



def downgrade() -> None:

    op.drop_table('repasse_prestador')
    op.execute("DROP SEQUENCE public.id_repasse_prestador_seq")

