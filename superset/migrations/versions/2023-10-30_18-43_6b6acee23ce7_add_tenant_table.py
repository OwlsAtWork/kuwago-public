# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""add_tenant_table

Revision ID: 6b6acee23ce7
Revises: 4b85906e5b91
Create Date: 2023-10-30 18:43:15.723522

"""

# revision identifiers, used by Alembic.
revision = '6b6acee23ce7'
down_revision = '4b85906e5b91'

from alembic import op
import sqlalchemy as sa


def upgrade():
    op.create_table("ab_tenant",
        sa.Column("id", sa.INTEGER(), autoincrement=True, nullable=False),
        sa.Column("name", sa.VARCHAR(length=255), autoincrement=False, nullable=False),
        sa.Column("db_name", sa.VARCHAR(length=255), autoincrement=False, nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_table("ab_user_tenant",
        sa.Column("id", sa.INTEGER(), autoincrement=True, nullable=False),
        sa.Column("user_id", sa.INTEGER(), autoincrement=False, nullable=False),
        sa.Column("tenant_id", sa.INTEGER(), autoincrement=False, nullable=False),
        sa.ForeignKeyConstraint(["tenant_id"], ["ab_tenant.id"]),
        sa.ForeignKeyConstraint(["user_id"], ["ab_user.id"]),
        sa.PrimaryKeyConstraint("id"),
    )


def downgrade():
    op.drop_table("ab_user_tenant")
    op.drop_table("ab_tenant")
