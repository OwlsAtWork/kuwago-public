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
# pylint: disable=too-few-public-methods, invalid-name
from __future__ import annotations

import dataclasses
import logging
from abc import ABC
from typing import Any, Callable, TYPE_CHECKING

from flask_babel import gettext as __

from superset.errors import ErrorLevel, SupersetError, SupersetErrorType
from superset.exceptions import SupersetErrorException
from superset.sqllab.command_status import SqlJsonExecutionStatus
from superset.utils import core as utils

if TYPE_CHECKING:
    from superset.sqllab.sqllab_execution_context import SqlJsonExecutionContext


QueryStatus = utils.QueryStatus
logger = logging.getLogger(__name__)

ExecuteTask = Callable[..., SqlJsonExecutionStatus]


class SqlExecutor:
    def execute(
        self,
        execution_context: SqlJsonExecutionContext,
        query_id: str,
        sqllab_ctas_no_limit_flag: bool,
        log_params: dict[str, Any] | None = None,
    ) -> SqlJsonExecutionStatus:
        raise NotImplementedError()


class SqlExecutorBase(SqlExecutor, ABC):
    _execute_task: ExecuteTask

    def __init__(self, execute_task: ExecuteTask):
        self._execute_task = execute_task


class SynchronousSqlExecutor(SqlExecutorBase):
    _timeout_duration_in_seconds: int
    _sqllab_backend_persistence_feature_enable: bool

    def __init__(
        self,
        execute_task: ExecuteTask,
        timeout_duration_in_seconds: int,
        sqllab_backend_persistence_feature_enable: bool,
    ):
        super().__init__(execute_task)
        self._timeout_duration_in_seconds = timeout_duration_in_seconds
        self._sqllab_backend_persistence_feature_enable = (
            sqllab_backend_persistence_feature_enable
        )

    def execute(
        self,
        execution_context: SqlJsonExecutionContext,
        query_id: str,
        sqllab_ctas_no_limit_flag: bool,
        log_params: dict[str, Any] | None = None,
    ) -> SqlJsonExecutionStatus:
        return self._execute_with_timeout(
                execution_context,
                query_id,
                sqllab_ctas_no_limit_flag,
                log_params=log_params,
            )

    def _execute_with_timeout(
        self,
        execution_context: SqlJsonExecutionContext,
        query_id: str,
        sqllab_ctas_no_limit_flag: bool,
        log_params: dict[str, Any] | None = None,
    ) -> SqlJsonExecutionStatus:
        with utils.timeout(
            seconds=self._timeout_duration_in_seconds,
            error_message=self._get_timeout_error_msg(),
        ):
            return self._execute(
                execution_context,
                query_id,
                sqllab_ctas_no_limit_flag,
                log_params=log_params,
            )

    def _execute(
        self,
        execution_context: SqlJsonExecutionContext,
        query_id: str,
        sqllab_ctas_no_limit_flag: bool,
        log_params: dict[str, Any] | None = None,
    ) -> SqlJsonExecutionStatus:
        return self._execute_task(
            execution_context,
            query_id,
            sqllab_ctas_no_limit_flag,
            log_params=log_params,
        )

    def _get_timeout_error_msg(self) -> str:
        return (
            f"The query exceeded the {self._timeout_duration_in_seconds} "
            "seconds timeout."
        )


class ASynchronousSqlExecutor(SqlExecutorBase):
    def execute(
        self,
        execution_context: SqlJsonExecutionContext,
        query_id: str,
        sqllab_ctas_no_limit_flag: bool,
        log_params: dict[str, Any] | None = None,
    ) -> SqlJsonExecutionStatus:
        logger.info("Query %i: Running query on a Celery worker", query_id)
        try:
            task = self._execute_task.delay(  # type: ignore
                execution_context,
                query_id,
                sqllab_ctas_no_limit_flag,
                log_params=log_params,
            )
            try:
                task.forget()
            except NotImplementedError:
                logger.warning(
                    "Unable to forget Celery task as backend"
                    "does not support this operation"
                )
        except Exception as ex:
            logger.exception("Query %i: %s", query_id, str(ex))

            message = __("Failed to start remote query on a worker.")
            error = SupersetError(
                message=message,
                error_type=SupersetErrorType.ASYNC_WORKERS_ERROR,
                level=ErrorLevel.ERROR,
            )
            # error_payload = dataclasses.asdict(error)
            # query.set_extra_json_key("errors", [error_payload])
            # query.status = QueryStatus.FAILED
            # query.error_message = message
            raise SupersetErrorException(error) from ex
        return SqlJsonExecutionStatus.QUERY_IS_RUNNING
