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
from collections.abc import Iterator
from contextlib import contextmanager
import logging

from flask import g, request
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import NullPool

from superset.extensions import db
from superset.multi_tenancy.tenant import Tenant, UserTenant


# logging.basicConfig()
# logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)
logger = logging.getLogger(__name__)


REQUESTS_TO_DEFAULT = [
    {"endpoint": "users/add", "methods": ["POST"]},
    {"endpoint": "users/delete", "methods": ["POST"]},
    {"endpoint": "databases/add", "methods": ["POST"]},
    {"endpoint": "databases/delete", "methods": ["POST"]},
    {"endpoint": "login", "methods": []},
    {"endpoint": "logout", "methods": []}
]


@contextmanager
def session_scope(app) -> Iterator[Session]:
    """Provides a transactional scope around a series of operations"""
    database_uri = app.config["SQLALCHEMY_DATABASE_URI"]
    engine = create_engine(database_uri, poolclass=NullPool)
    session_class = sessionmaker()
    session_class.configure(bind=engine)
    session = session_class()

    try:
        yield session
        session.commit()
    except SQLAlchemyError as ex:
        session.rollback()
        logger.exception(ex)
        raise
    finally:
        session.close()


def get_tenant_session(app, tenant):
    uri_template = app.config["SQLALCHEMY_DATABASE_TEMPLATE"]
    bind_key = tenant["name"]
    db_name = tenant["db_name"]
    if bind_key not in app.config["SQLALCHEMY_BINDS"]:
        app.config["SQLALCHEMY_BINDS"][bind_key] = uri_template.format(db_name)
    engine = db.get_engine(app, bind=bind_key)
    session = db.create_scoped_session({"bind": engine})
    return session


def multitenant_flask_app_mutator(app):

    @app.before_request
    def before_request_func():
        print("ZEYNEP BEFORE REQUEST BEGIN", request.url, request.method, db.session)

        if any(
                req["endpoint"] in request.url and (
                    not req["methods"] or request.method in req["methods"])
                for req in REQUESTS_TO_DEFAULT):
            print("ZEYNEP IGNORING")
            print("ZEYNEP BEFORE REQUEST END", db.session)
            return

        if not hasattr(g, "user") or not g.user or g.user.is_anonymous:
            print("ZEYNEP IGNORING - ANONYMOUS USER")
            print("ZEYNEP BEFORE REQUEST END", db.session)
            return

        print("ZEYNEP user id", g.user)
        tenant = None
        with session_scope(app) as session:
            user_tenants = session.query(UserTenant).filter_by(user_id=g.user.id).all()
            if user_tenants:
                tenant_model = session.query(Tenant).filter_by(id=user_tenants[0].tenant_id).one()
                tenant = tenant_model.to_json()
                print("ZEYNEP tenant name db", tenant["name"], tenant["db_name"])
        if tenant:
            print("ZEYNEP BEFORE REQUEST SWITCHING TO TENANT SESSION")
            db.session = get_tenant_session(app, tenant)
        print("ZEYNEP BEFORE REQUEST END", db.session)

    @app.after_request
    def after_request_func(response):
        print("ZEYNEP AFTER REQUEST BEGIN", response, db.session, db.default_session)
        if db.default_session != db.session:
            print("ZEYNEP AFTER REQUEST SWITCHING FROM TENANT SESSION")
            db.session.remove()
            db.session = db.default_session
        print("ZEYNEP AFTER REQUEST END", db.session)
        return response
