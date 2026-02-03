#!/usr/bin/env python

"""
Copyright (C) 2025 Marius Kortmann

This program is free software: you can redistribute it and/or modify it under the terms of the GNU Affero General Public License as published by the Free Software Foundation, either version 3 of the License, or any later version.

This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License along with this program. If not, see <https://www.gnu.org/licenses/>
"""

from typing import Callable

import threading
import time

from threading import Thread, Condition

import rclpy
from rclpy.node import Node
from rclpy.callback_groups import MutuallyExclusiveCallbackGroup, ReentrantCallbackGroup

from lifecycle_msgs.srv import ChangeState, GetState
from lifecycle_msgs.msg import Transition

from rcl_interfaces.srv import SetParameters
from rcl_interfaces.msg import Parameter, ParameterValue
from rcl_interfaces.msg import ParameterType

from std_msgs.msg import String

from rclpy.qos import DurabilityPolicy, ReliabilityPolicy, QoSProfile
from stars_msgs.msg import StarsSupervisor, StarsReady

# TODO

# TODO NOTES
#   - New Road for every Lanelet? Just group neighbors
#   - Walkways from Area to Lanelet
#   - Crosswalks???
#   For Prädikate
#   - Remember to evaluate the main points that the creators of interaction had
#   - Time movin vs Time standing still
#   - Distance to other agents for Criticality
class StarsSupervisor(Node):

    def __init__(self, node_name: str, workers: dict, executor, context) -> None:
        """Creates a ROS2 topic subscription listening for the current map data and calling _write_static_data_to_file
            to write it to disk"""
        super().__init__(node_name, context=context)
        self.ctx = context
        self.executor = executor

        self.manage_cb_group = ReentrantCallbackGroup()
        self.supervision_cb_group = MutuallyExclusiveCallbackGroup()
        self.report_cb_group = MutuallyExclusiveCallbackGroup()
        self.done_cb_group = ReentrantCallbackGroup()

        self.lock = threading.Lock()
        self.current_map = ""
        self.current_scenario = ""
        self.scenario_finished = threading.Event()
        self.finished = threading.Event()
        self.ready = threading.Event()

        self.workers = workers
        self.worker_names = workers.keys()

        self._supervision_started = False

        self.expected = set(self.worker_names)
        self.done = set()
        self.done_condition = Condition()

        self.done_subscriber = self.create_subscription(String, '/stars/receive/workers_done', self._on_done, 10)

        # service clients per worker
        self.cli = {}
        for name in self.worker_names:
            self.cli[name] = {
                'change': self.create_client(ChangeState, f'/{name}/change_state', callback_group = self.manage_cb_group),
                'state':  self.create_client(GetState,    f'/{name}/get_state', callback_group = self.manage_cb_group),
                'params': self.create_client(SetParameters, f'/{name}/set_parameters', callback_group = self.manage_cb_group),
            }

        # Create subscriber for Supervisor Messages
        callback: Callable[[StarsSupervisor], None] = lambda scenario_info: self.__save_scenario(scenario_info=scenario_info)
        self.create_subscription(
            msg_type=StarsSupervisor, topic="/stars/supervisor",
            callback=callback,
            qos_profile = QoSProfile(depth=1, reliability=ReliabilityPolicy.RELIABLE, durability = DurabilityPolicy.TRANSIENT_LOCAL),
            callback_group = None)

        # Create publisher for ready_to_receive message
        self.publisher = self.create_publisher(
            msg_type=StarsReady, topic="/stars/supervisor/ready",
            qos_profile = QoSProfile(depth=1, reliability=ReliabilityPolicy.RELIABLE, durability = DurabilityPolicy.TRANSIENT_LOCAL),
            callback_group = None
            )

        self.thread = Thread(target=self.__supervise)

        self.thread.start()

        self.get_logger().info(message=f"Successfully created. Starting Supervisor.")

    # --- Supervision of Worker Nodes
    def __supervise(self):
        try:
            while not self.finished.is_set() and rclpy.ok(context=self.ctx):
                # wait until all services are up once
                if not self._supervision_started:
                    ready = all(
                        self.cli[worker]['change'].wait_for_service(timeout_sec=0.0) and
                        self.cli[worker]['state'].wait_for_service(timeout_sec=0.0) and
                        self.cli[worker]['params'].wait_for_service(timeout_sec=0.0)
                        for worker in self.worker_names
                    )
                    if not ready:
                        break
                    self._supervision_started = True
                    self.get_logger().info('All lifecycle and parameter services are available')

                # Cleanup after finish
                if self.finished.is_set():
                    self.get_logger().info('All scenarios processed. Shutting down supervisor.')

                    for worker in self.worker_names:
                        self._set_state(worker, Transition.TRANSITION_DESTROY)

                    return

                # Send ready_to_receive message
                ready_msg = StarsReady()
                ready_msg.ready_to_receive = True
                self.publisher.publish(ready_msg)
                self.get_logger().info(f'Stars ROS Exporter ready to receive next scenario.')

                if self.scenario_finished.is_set():
                    self.get_logger().info('Waiting for next Scenario...')
                    break

                scenario = ""
                map_name = ""
                # Update current Scenario
                with self.lock:
                    scenario = self.current_map + "-" + self.current_scenario
                    map_name = self.current_map

                # 1) Set input_path on all workers (while inactive/unconfigured)
                for worker in self.worker_names:
                    if not self._set_scenario(worker, scenario, map_name):
                        self.get_logger().error(f'Failed to set Scenario on {worker}')
                        return
                    self.get_logger().info(f'Scenario set on {worker}')

                # 2) CONFIGURE -> ACTIVATE all
                for worker in self.worker_names:
                    if not self._set_state(worker, Transition.TRANSITION_CONFIGURE):
                        self.get_logger().error(f'{worker} could not be CONFIGURED')
                        return
                    self.get_logger().info(f'{worker} configured')
                self.get_logger().info(f'All workers configured')
                for worker in self.worker_names:
                    if not self._set_state(worker, Transition.TRANSITION_ACTIVATE):
                        self.get_logger().error(f'{worker} could not be ACTIVATED')
                        return
                    self.get_logger().info(f'{worker} activated')
                self.get_logger().info(f'All workers activated')

                # 3) Wait for all three to report done
                with self.done_condition:
                    self.done.clear()
                    while self.done != self.expected and rclpy.ok(context=self.ctx):
                        self.get_logger().info(f"Waiting: done={sorted(self.done)} expected={sorted(self.expected)}")
                        self.done_condition.wait(timeout=5.0)

                # 4) DEACTIVATE -> CLEANUP all
                for worker in self.worker_names:
                    self._set_state(worker, Transition.TRANSITION_DEACTIVATE)
                for worker in self.worker_names:
                    self._set_state(worker, Transition.TRANSITION_CLEANUP)

                # Pause Timer to allow receiving nodes to prepare for a new Scene
                self.get_logger().info('Cycle complete.')
        except Exception as e:
            self.get_logger().error(f'Exception in supervision loop: {e}\n{traceback.format_exc()}')

    def _on_done(self, msg: String) -> None:
        try:
            # self.done.add(list(self.workers.keys())[list(self.workers.values()).index(str(msg.data))])
            self.get_logger().info(f"_on_done received: '{msg.data}'")
            worker = list(self.workers.keys())[list(self.workers.values()).index(str(msg.data))]
            with self.done_condition:
                self.done.add(worker)
                self.done_condition.notify_all()
        except Exception as e:
            self.get_logger().error(f'Exception in _on_done: {e}\n{traceback.format_exc()}')

    # --- Save Scenario Info

    def __save_scenario(self, scenario_info: StarsSupervisor) -> None:
        with self.lock:
            self.current_map = scenario_info.scenario_map_name
            self.current_scenario = scenario_info.scenario_name
            self.scenario_finished = scenario_info.scenario_finished
            self.finished = scenario_info.finished

    # --- Helpers

    def _set_state(self, node_name: str, transition_id: int) -> bool:
        try:
            cli = self.cli[node_name]['change']
            req = ChangeState.Request()
            req.transition.id = transition_id
            fut = cli.call_async(req)

            if not self._wait_future(fut, timeout_sec=5.0):
                self.get_logger().error(f"Timeout while changing state of {node_name}")
                return False

            res = fut.result()
            return bool(res and res.success)
        except Exception as e:
            self.get_logger().error(f'Exception while changing state of {node_name}: {e}\n{traceback.format_exc()}')
            return False

    def _set_scenario(self, node_name: str, scenario: str, map_name: str) -> bool:
        self.get_logger().info(f'Setting new Scenario on {node_name}')
        cli = self.cli[node_name]['params']
        pval_scenario = ParameterValue(
            type=ParameterType.PARAMETER_STRING,
            string_value=scenario
        )
        pval_map = ParameterValue(
            type=ParameterType.PARAMETER_STRING,
            string_value=map_name
        )
        param_scenario = Parameter(name='scenario', value=pval_scenario)
        param_map = Parameter(name='map_name', value=pval_map)
        req = SetParameters.Request(parameters=[param_scenario, param_map])
        fut = cli.call_async(req)

        if not self._wait_future(fut, timeout_sec=5.0):
            self.get_logger().error(f"Timeout while setting scenario on {node_name}")
            return False

        res = fut.result()
        return bool(res and all([r.successful for r in res.results]))

    def _wait_future(self, fut, timeout_sec: float) -> bool:
        done_evt = threading.Event()

        def _done_cb(_):
            done_evt.set()

        fut.add_done_callback(_done_cb)

        start = time.monotonic()
        while rclpy.ok(context=self.ctx):
            remaining = timeout_sec - (time.monotonic() - start)
            if remaining <= 0:
                return False
            if done_evt.wait(timeout=min(0.1, remaining)):
                return True
        return False