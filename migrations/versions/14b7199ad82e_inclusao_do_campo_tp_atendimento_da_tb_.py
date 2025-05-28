"""Inclusao do campo TP_ATENDIMENTO da tb ATENDIME

Revision ID: 14b7199ad82e
Revises: 4e8ab80fb67c
Create Date: 2025-05-27 16:41:15.506322

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '14b7199ad82e'
down_revision: Union[str, None] = '4e8ab80fb67c'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:

    op.add_column('atendime', sa.Column('TP_ATENDIMENTO', sa.String(), nullable=True))


def downgrade() -> None:

    op.drop_column('atendime', 'TP_ATENDIMENTO')

