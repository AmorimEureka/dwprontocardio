"""Inclusao dos campos reg_amb e reg_fat

Revision ID: 03f4a8600026
Revises: a46b6f2040af
Create Date: 2025-04-17 14:35:35.172392

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '03f4a8600026'
down_revision: Union[str, None] = 'a46b6f2040af'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:


    op.add_column('reg_amb', sa.Column('DT_REG_AMB', sa.DateTime(), nullable=True))
    op.add_column('reg_fat', sa.Column('DT_REG_FAT', sa.DateTime(), nullable=True))



def downgrade() -> None:

    op.drop_column('reg_fat', 'DT_REG_FAT')
    op.drop_column('reg_amb', 'DT_REG_AMB')

