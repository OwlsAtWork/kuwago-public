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
from flask_appbuilder import Model
from sqlalchemy import Column, ForeignKey, Integer, Sequence, String


class Tenant(Model):

    """Tenant information"""

    __tablename__ = "ab_tenant"
    id = Column(Integer, Sequence("ab_tenant_id_seq"), primary_key=True)
    name = Column(String(255), nullable=False)
    db_name = Column(String(255), nullable=False)


class UserTenant(Model):

    """Mapping between the user and the tenant"""

    __tablename__ = "ab_user_tenant"
    id = Column(Integer, Sequence("ab_user_tenant_id_seq"), primary_key=True)
    user_id = Column(String(255), ForeignKey("user.id"))
    tenant_id = Column(Integer, ForeignKey("tenant.id"))
