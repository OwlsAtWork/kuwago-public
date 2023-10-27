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
import logging

from flask import g
from flask_appbuilder import SQLA
from flask_appbuilder.models.sqla import Model
from flask_sqlalchemy import (
    BaseQuery,
    get_state,
    SessionBase,
    SignallingSession,
)
from sqlalchemy import orm

log = logging.getLogger(__name__)


class MultiTenantSignallingSession(SignallingSession):

    """Implements signalling session supporting multi-tenancy.
    Taken from CustomSignallingSession in flask_appbuilder/models/sqla/__init__.py
    """

    def get_bind(self, mapper=None, *args, **kwargs):
        """Return the engine or connection for a given model or
        table, using the ``__bind_key__`` if it is set.

        Patch from https://github.com/pallets/flask-sqlalchemy/pull/1001
        """
        # mapper is None if someone tries to just get a connection
        if mapper is not None:
            try:
                # SA >= 1.3
                persist_selectable = mapper.persist_selectable
            except AttributeError:
                # SA < 1.3
                persist_selectable = mapper.mapped_table
            info = getattr(persist_selectable, "info", {})
            tenant_name = g.tenant_name if hasattr(g, "tenant_name") else None
            bind_key = info.get("bind_key") or tenant_name
            if bind_key is not None:
                state = get_state(self.app)
                return state.db.get_engine(self.app, bind=bind_key)
        return SessionBase.get_bind(self, mapper, *args, **kwargs)


class MultiTenantSQLA(SQLA):

    """Implements SQLAlchemy wrapper supporting multi-tenancy"""

    def __init__(self, app=None, use_native_unicode=True, session_options=None,
                 metadata=None, query_class=BaseQuery, model_class=Model,
                 engine_options=None):
        super().__init__(
            app=app, use_native_unicode=use_native_unicode,
            session_options=session_options, metadata=metadata,
            query_class=query_class, model_class=model_class,
            engine_options=engine_options)
        self.default_session = self.session

    def create_session(self, options):
        """Custom Session factory to support multi-tenancy"""
        return orm.sessionmaker(class_=MultiTenantSignallingSession, db=self, **options)
