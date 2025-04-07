"""inclusão da tabela procedimento SUS e CD_PROCEDIMENTO

Revision ID: a46b6f2040af
Revises: deb1a7e654e0
Create Date: 2025-03-17 18:05:53.469697

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'a46b6f2040af'
down_revision: Union[str, None] = 'deb1a7e654e0'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:

    op.execute("CREATE SEQUENCE public.id_procedimento_sus_seq")
    op.create_table('procedimento_sus',
    sa.Column('id_procedimento_sus', sa.BigInteger(), server_default=sa.text("nextval('public.id_procedimento_sus_seq'::regclass)"), nullable=False),
    sa.Column('CD_PROCEDIMENTO', sa.String(), nullable=True),
    sa.Column('DS_PROCEDIMENTO', sa.String(), nullable=True),
    sa.Column('DT_EXTRACAO', sa.DateTime(), server_default=sa.text('now()'), nullable=True),
    sa.PrimaryKeyConstraint('id_procedimento_sus')
    )

    op.add_column('itreg_fat', sa.Column('CD_PROCEDIMENTO', sa.String(), nullable=True))



def downgrade() -> None:

    op.drop_column('itreg_fat', 'CD_PROCEDIMENTO')
    op.drop_table('procedimento_sus')
    op.execute("DROP SEQUENCE public.id_procedimento_sus_seq")
    # ### end Alembic commands ###
