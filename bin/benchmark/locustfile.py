#!/usr/bin/env python3
# Copyright (c) -2023 curoky(cccuroky@gmail.com).
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
import locust
import thriftpy2

from thriftoy.LocustUser import ThriftUser

echo_thrift = thriftpy2.load("../echo/echo.thrift", module_name="echo_thrift")


class MyThriftUser(ThriftUser):
    wait_time = locust.between(0.9, 1.0)

    service = echo_thrift.EchoService
    ip = "0.0.0.0"
    port = 6000

    @locust.task
    def echo(self):
        self.call("echo", "hhh")
