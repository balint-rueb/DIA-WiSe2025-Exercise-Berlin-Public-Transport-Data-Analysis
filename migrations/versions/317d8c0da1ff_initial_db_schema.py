"""initial db schema

Revision ID: 317d8c0da1ff
Revises: 
Create Date: 2025-12-13 19:11:41.623585

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
import os


# revision identifiers, used by Alembic.
revision: str = '317d8c0da1ff'
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Get the directory where this migration file is located
    dir_path = os.path.dirname(os.path.realpath(__file__))
    
    # Build the full path to your SQL file
    file_path = os.path.join(dir_path, 'star_schema.sql')
    
    # Read and execute
    with open(file_path, 'r') as file:
        sql_statements = file.read()
        op.execute(sql_statements)

def downgrade() -> None:
    # You can also have a 'drop_schema.sql' if you want, 
    # or just keep the raw SQL here since dropping is usually short.
    op.execute("""
        DROP TABLE IF EXISTS fact_train_stops;
        DROP TABLE IF EXISTS dim_time;
        DROP TABLE IF EXISTS dim_train_profiles;
        DROP TABLE IF EXISTS dim_stations;
    """)
