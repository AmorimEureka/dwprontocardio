"""Inclusao valor total SUS

Revision ID: fc18a2c7e992
Revises: 2a9fd0e7d055
Create Date: 2025-05-05 20:27:38.127859

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'fc18a2c7e992'
down_revision: Union[str, None] = '2a9fd0e7d055'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:

    op.add_column('itreg_fat', sa.Column('VL_SP', sa.String(), nullable=True))
    op.add_column('itreg_fat', sa.Column('VL_ATO', sa.String(), nullable=True))



def downgrade() -> None:

    op.drop_column('itreg_fat', 'VL_ATO')
    op.drop_column('itreg_fat', 'VL_SP')

