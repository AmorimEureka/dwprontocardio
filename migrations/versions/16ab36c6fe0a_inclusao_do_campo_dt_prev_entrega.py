"""Inclusao do campo DT_PREV_ENTREGA

Revision ID: 16ab36c6fe0a
Revises: 14b7199ad82e
Create Date: 2025-06-22 20:31:23.936988

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '16ab36c6fe0a'
down_revision: Union[str, None] = '14b7199ad82e'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:

    op.add_column('ord_com', sa.Column('DT_PREV_ENTREGA', sa.DateTime(), nullable=True))



def downgrade() -> None:

    op.drop_column('ord_com', 'DT_PREV_ENTREGA')

