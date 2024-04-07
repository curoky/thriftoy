# Copyright (c) 2022-2024 curoky(cccuroky@gmail.com).
#
# This file is part of thriftoy.
# See https://github.com/curoky/thriftoy for further info.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
import multiprocessing
import queue
from collections.abc import Callable
from typing import Any

import sqlalchemy
import sqlmodel

from ..common.message import TMessage


class MultiProcessStreamingTransformer:
    def __init__(self, transform: Callable[[TMessage], Any], num_processes=None):
        self.transform = transform
        self.num_processes = num_processes
        self.pool = multiprocessing.Pool(num_processes)
        self.queue = queue.Queue()

    def push(self, message: TMessage):
        self.queue.put(self.pool.apply_async(self.transform, (message,)))

    def get(self):
        return self.queue.get(True)


class MultiProcessBatchingTransformer:
    def __init__(self, transform: Callable[[TMessage], Any], num_processes=None):
        self.transform = transform
        self.num_processes = num_processes
        self.pool = multiprocessing.Pool(num_processes)

    def run(self, source_engine: sqlalchemy.Engine, target_engine: sqlalchemy.Engine, limit=None):
        with sqlmodel.Session(source_engine) as session:
            statement = sqlmodel.select(TMessage)
            if limit is not None:
                statement = statement.limit(limit)
            messages = self.pool.map(self.transform, session.exec(statement))
        count = 0
        with sqlmodel.Session(target_engine) as session:
            for message in messages:
                count += len(message)
                session.add_all(message)
            session.commit()
        logging.info("MultiProcessBatchingTransformer: %d messages transformed", count)


def batch_transform(
    source_engine,
    target_engine,
    transform=Callable[[TMessage], Any],
    num_processes=None,
    limit=None,
):
    transformer = MultiProcessBatchingTransformer(transform=transform, num_processes=num_processes)
    transformer.run(source_engine, target_engine, limit=limit)
