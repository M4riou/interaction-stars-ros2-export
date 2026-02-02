"""
Copyright (C) 2024 Valentin Rusche, Marius Kortmann

This program is free software: you can redistribute it and/or modify it under the terms of the GNU Affero General Public License as published by the Free Software Foundation, either version 3 of the License, or any later version.

This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License along with this program. If not, see <https://www.gnu.org/licenses/>
"""

import rclpy
import threading
import time
from rclpy.node import Node
from rclpy.client import Client
from rclpy.task import Future


class AsyncServiceClient(Node):

    def __init__(self, node_name: str, message_type, topic_name: str, callback_group, timeout_sec: float = 10.0) -> None:
        # Create a client
        super().__init__(node_name=node_name, parameter_overrides=[])
        self.timeout: float = timeout_sec
        self.client: Client = self.create_client(srv_type=message_type, srv_name=topic_name, callback_group=callback_group)

        # Check if the a service is available
        while not self.client.wait_for_service(timeout_sec=self.timeout):
            self.get_logger().info(message="Waiting for available service.")

        self.message_type = message_type

        self.request = self.message_type.Request()

    def send_request(self):
        self.response: Future = self.client.call_async(request=self.request)

        if not self._wait_future(self.response, timeout_sec=5.0):
            self.get_logger().error(f"Timeout while waiting for service response")
            return False

        return self.response.result()

    def _wait_future(self, fut, timeout_sec: float) -> bool:
        done_evt = threading.Event()

        def _done_cb(_):
            done_evt.set()

        fut.add_done_callback(_done_cb)

        start = time.monotonic()
        while rclpy.ok(context=self.context):
            remaining = timeout_sec - (time.monotonic() - start)
            if remaining <= 0:
                return False
            if done_evt.wait(timeout=min(0.1, remaining)):
                return True
        return False
