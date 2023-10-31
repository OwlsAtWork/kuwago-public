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
"""add_is_nl_query_to_tab_state_view

Revision ID: e260a3470e74
Revises: 4b85906e5b91
Create Date: 2023-10-31 10:47:22.389975

"""

# revision identifiers, used by Alembic.
revision = 'e260a3470e74'
down_revision = '4b85906e5b91'

from alembic import op
import sqlalchemy as sa


def upgrade():
    op.add_column("tab_state", sa.Column("is_nl_query", sa.Boolean(), nullable=True))


def downgrade():
    op.drop_column("tab_state", "is_nl_query")
