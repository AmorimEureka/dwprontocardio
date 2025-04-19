"""Inclusao campo CD_PRESTADOR na tabela ATENDIME

Revision ID: 2a9fd0e7d055
Revises: 03f4a8600026
Create Date: 2025-04-19 18:33:38.772617

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '2a9fd0e7d055'
down_revision: Union[str, None] = '03f4a8600026'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:

    op.add_column('atendime', sa.Column('CD_PRESTADOR', sa.String(), nullable=True))



def downgrade() -> None:

    op.drop_column('atendime', 'CD_PRESTADOR')

