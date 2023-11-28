# Copyright (c) 2023-2023 curoky(cccuroky@gmail.com).
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

import queue
from collections.abc import Callable
from typing import Any


class ObjectPool:
    def __init__(self, creator: Callable[[], Any], pool_size: int):
        self.pool = queue.Queue(pool_size)
        self.creator = creator

    def get(self):
        try:
            return self.pool.get_nowait()
        except Exception:
            return self.creator()

    def put(self, item):
        try:
            self.pool.put_nowait(item)
        except Exception:
            pass
