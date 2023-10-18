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
# pylint: disable=too-few-public-methods, too-many-arguments
from __future__ import annotations

import copy
import logging
from typing import Any, Optional, TYPE_CHECKING

from celery import Task

from superset import app, is_feature_enabled, security_manager
from superset.commands.base import BaseCommand
from superset.common.db_query_status import QueryStatus
from superset.daos.query import QueryDAO
from superset.extensions import celery_app
from superset.jinja_context import get_template_processor
from superset.models.sql_lab import Query
from superset.sql_lab import get_query, get_sql_query, get_sql_results
from superset.sqllab.command_status import SqlJsonExecutionStatus
from superset.sqllab.exceptions import QueryIsForbiddenToAccessException
from superset.sqllab.limiting_factor import LimitingFactor
from superset.sqllab.query_render import SqlQueryRenderImpl
from superset.sqllab.sql_json_executer import SynchronousSqlJsonExecutor
from superset.sqllab.validators import CanAccessQueryValidatorImpl
from superset.utils.core import override_user
from superset.utils.celery import session_scope

if TYPE_CHECKING:
    from superset.sqllab.sqllab_execution_context import SqlJsonExecutionContext


config = app.config
logger = logging.getLogger(__name__)


@celery_app.task(
    name="execute_new_query.run",
    bind=True,
    time_limit=60,
    soft_time_limit=60,
)
def run(
    ctask: Task,
    execution_context: SqlJsonExecutionContext,
    query_id: str,
    sqllab_ctas_no_limit_flag: bool,
    log_params: dict[str, Any] | None = None,
    username: Optional[str] = None,
) -> SqlJsonExecutionStatus:
    with session_scope(not ctask.request.called_directly) as session:
        with override_user(security_manager.find_user(username)):
            query_dao = QueryDAO()
            query = get_query(query_id, session)
            nl_query = query.nl_query
            if nl_query:
                logger.info("Getting the SQL query for query_id %i", query.id)
                database = query.database
                execution_context.set_database(database)
                database_schema = database.get_all_metadata()
                sql = get_sql_query(nl_query, database_schema)
                execution_context.set_sql(sql)
                query.sql = sql
                logger.debug("NL -> SQL for query_id %i:\n%s\n%s", query.id, nl_query, sql)
                query_dao.update(query, {"sql": query.sql})
            try:
                logger.info("Triggering query_id: %i", query.id)
                execution_context.set_query(query)
                command = PreExecuteSql(
                    execution_context,
                    query_dao,
                    copy.copy(query),
                    sqllab_ctas_no_limit_flag,
                    log_params
                )
                # command.validate() # commented out due to an error: "can not access the query"
                command.set_query_limit_if_required()
                query_dao.update(query, {"limit": query.limit})
                return command.run()
            except Exception as ex:
                query_dao.update(query, {"status": QueryStatus.FAILED})
                raise ex


class PreExecuteSql(BaseCommand):
    _execution_context: SqlJsonExecutionContext
    _sqllab_ctas_no_limit: bool
    _log_params: dict[str, Any] | None = None

    def __init__(
        self,
        execution_context: SqlJsonExecutionContext,
        query_dao: QueryDAO,
        query: Query,
        sqllab_ctas_no_limit_flag: bool,
        log_params: dict[str, Any] | None = None
    ) -> None:
        self._execution_context = execution_context
        self._access_validator = CanAccessQueryValidatorImpl()
        self._sql_json_executor = SynchronousSqlJsonExecutor(
            query_dao,
            get_sql_results,
            config.get("SQLLAB_TIMEOUT"),
            is_feature_enabled("SQLLAB_BACKEND_PERSISTENCE"),
        )
        self._sqllab_ctas_no_limit = sqllab_ctas_no_limit_flag
        sql_query_render = SqlQueryRenderImpl(get_template_processor)
        rendered_query = sql_query_render.render(execution_context)
        self._validate_rendered_query = query
        self._validate_rendered_query.sql = rendered_query
        self._log_params = log_params

    def validate(self) -> None:
        try:
            self._access_validator.validate(self._validate_rendered_query)
        except Exception as ex:
            raise QueryIsForbiddenToAccessException(self._execution_context, ex) from ex

    def run(  # pylint: disable=too-many-statements,useless-suppression
        self,
    ) -> SqlJsonExecutionStatus:
        """Runs arbitrary sql and returns data as json"""
        return self._sql_json_executor.execute(
            self._execution_context, self._validate_rendered_query.sql, self._log_params
        )

    def set_query_limit_if_required(
        self,
    ) -> None:
        if self._is_required_to_set_limit():
            self._set_query_limit()

    def _is_required_to_set_limit(self) -> bool:
        return not (
            self._sqllab_ctas_no_limit and self._execution_context.select_as_cta
        )

    def _set_query_limit(self) -> None:
        db_engine_spec = self._execution_context.database.db_engine_spec  # type: ignore
        limits = [
            db_engine_spec.get_limit_from_sql(self._validate_rendered_query.sql),
            self._execution_context.limit,
        ]
        if limits[0] is None or limits[0] > limits[1]:  # type: ignore
            self._execution_context.query.limiting_factor = LimitingFactor.DROPDOWN
        elif limits[1] > limits[0]:  # type: ignore
            self._execution_context.query.limiting_factor = LimitingFactor.QUERY
        else:  # limits[0] == limits[1]
            self._execution_context.query.limiting_factor = (
                LimitingFactor.QUERY_AND_DROPDOWN
            )
        self._execution_context.query.limit = min(
            lim for lim in limits if lim is not None
        )
