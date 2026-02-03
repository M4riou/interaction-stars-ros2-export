"""
Copyright (C) 2024 Valentin Rusche, Marius Kortmann

This program is free software: you can redistribute it and/or modify it under the terms of the GNU Affero General Public License as published by the Free Software Foundation, either version 3 of the License, or any later version.

This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License along with this program. If not, see <https://www.gnu.org/licenses/>
"""

import os
from typing import Callable
from pathlib import Path
from rclpy.lifecycle import Node as LifecycleNode
from rclpy.lifecycle import State
from rclpy.lifecycle import TransitionCallbackReturn as TCR
from rcl_interfaces.msg import ParameterType
from rclpy.qos import DurabilityPolicy, ReliabilityPolicy, QoSProfile
from rclpy.impl.rcutils_logger import RcutilsLogger
from stars_msgs.msg import StarsWorldInfo
from stars_msgs.srv import StarsGetAllWaypoints
from std_msgs.msg import String
from ...stars import dataclass_to_json_converter
from .stars_waypoint_client import StarsWaypointClient


class StarsStaticMapReader(LifecycleNode):

    def __init__(self, node_name: str, uuid: str, polling_rate: int, callback_group, context) -> None:
        """Creates a ROS2 topic subscription listening for the current map data and calling _write_static_data_to_file
            to write it to disk"""
        super().__init__(node_name=node_name, context=context)
        self._uuid = uuid
        self.ctx = context
        self.polling_rate: int = polling_rate
        self.callback_group = callback_group

        self.declare_parameter('scenario', "")
        self.declare_parameter('map_name', "")

        self.is_exporting_done = False
        self.received_world_info = False

        self.callback: Callable[[StarsWorldInfo], None] = lambda world_info: self.__save_world_info(world_info=world_info)
        self.subscription = None

        self.waypoint_client: StarsWaypointClient = None
        self.done_publisher = self.create_publisher(String, '/stars/receive/workers_done', 10)

        self._timer = None

        self.get_logger().info(message=f"Successfully created")

    # --- Lifecycle hooks
    def on_configure(self, state: State) -> TCR:
        scenario = self.get_parameter('scenario').get_parameter_value().string_value
        map_name = self.get_parameter('map_name').get_parameter_value().string_value

        if not scenario or not map_name:
            self.get_logger().error('Scenario information not set')
            return TCR.FAILURE

        return TCR.SUCCESS

    def on_activate(self, state: State) -> TCR:
        # start reading/publishing
        self._timer = self.create_timer(self.polling_rate, self.__update_thread)

        self.waypoint_client = StarsWaypointClient(node_name = 'Stars_Waypoint_Client', message_type = StarsGetAllWaypoints,
                                                                topic_name = '/stars/static/waypoints/get_all_waypoints',
                                                                callback_group = self.callback_group, context=self.ctx, timeout_sec = 10.0)

        self.subscription = self.create_subscription(
                            msg_type=StarsWorldInfo, topic="/stars/static/world_info",
                            callback=self.callback,
                            qos_profile = QoSProfile(depth=1, reliability=ReliabilityPolicy.RELIABLE, durability = DurabilityPolicy.TRANSIENT_LOCAL),
                            callback_group = self.callback_group)

        # Check if the a service is available
        while not self.waypoint_client.wait_for_service(timeout_sec=10.0):
            self.get_logger().info(message="Waiting for available service.")

        self.get_logger().info('Stars_Static_Map_Reader activated')
        return TCR.SUCCESS

    def on_deactivate(self, state: State) -> TCR:
        if self._timer:
            self._timer.cancel()
            self._timer = None

        if self.waypoint_client:
            self.destroy_client(self.waypoint_client)
            self.waypoint_client = None

        if self.subscription:
            self.destroy_subscription(self.subscription)
            self.subscription = None

        self.get_logger().info('Stars_Static_Map_Reader deactivated')
        return TCR.SUCCESS

    def on_cleanup(self, state: State) -> TCR:
        self.get_logger().info('Stars_Static_Map_Reader cleaned up')
        return TCR.SUCCESS

    def on_shutdown(self, state: State) -> TCR:
        if self._timer:
            self._timer.cancel()
            self._timer = None
        if self.waypoint_client:
            self.destroy_client(self.waypoint_client)
            self.waypoint_client = None
        if self.subscription:
            self.destroy_subscription(self.subscription)
            self.subscription = None
        self.get_logger().info('Stars_Static_Map_Reader shutting down')
        return TCR.SUCCESS

    # --- Main Work
    def __save_world_info(self, world_info: StarsWorldInfo) -> None:
        self.get_logger().info(message="Received newest world info")
        self.world_info: StarsWorldInfo = world_info
        self.received_world_info = True

    def __update_thread(self) -> None:
        """
        execution loop for async mode actor discovery
        """
        if self.received_world_info and self.waypoint_client.waypoints is not None:
            self.__write_static_data_to_file(map_name=Path(self.world_info.map_name),
                                                map_data=self.world_info.map_data, map_format=self.world_info.map_format, logger = self.get_logger())

        self.get_logger().info(message="Done exporting static map data.")
        # Work was finished so we can stop the node from spinning infinitely
        self.__report_done()

    def __write_static_data_to_file(self, map_name: Path, map_data: str, map_format: str, logger: RcutilsLogger) -> None:
        """Creates a map file containing the read opendrive data xml string creating the desired path if it
        not yet exists. xml_dir is the name of the OpenDrive map dir under the SIMULATION_MAP_FILE_DIR path. 
        json_dir ist the name of the STARS compatible JSON file under the SIMULATION_STARS_STATIC_FILE_DIR path.
        """

        stars_json_dir: Path = Path(os.getenv(key="SIMULATION_STARS_STATIC_FILE_DIR")) / map_name.parent # type: ignore
        stars_json_file: Path = stars_json_dir / f"{str(map_name.name)}.json"

        if map_format == "xodr":
            self.__save_xodr_data(map_name=map_name, data=map_data)

            self.__save_as_stars_json(map_format=map_format,map_name=map_name, data=map_data, json_dir=stars_json_dir, json_file=stars_json_file)
        elif map_format == "lanelet2":
            self.__save_lanelet2_data(map_name=map_name, data=map_data)

            self.__save_as_stars_json(map_format=map_format,map_name=map_name, data=map_data, json_dir=stars_json_dir, json_file=stars_json_file)
        else:
            raise NotImplementedError(f"Map format {map_format} is not supported yet.")
        self.is_exporting_done = True

    # --- Helper
    def __save_as_stars_json(self, map_format: str, map_name: Path, data: str, json_dir: Path, json_file: Path) -> None:
        dataclass_to_json_converter.export_to_json(map_format=map_format,map_name=map_name, data=data, json_dir=json_dir, json_file=json_file, logger = self.get_logger(), waypoints=self.waypoint_client.waypoints)

    def __save_xodr_data(self, map_name: Path, data: str) -> None:

        xodr_dir: Path = Path(os.getenv(key="SIMULATION_MAP_FILE_DIR")) / map_name.parent # type: ignore
        xodr_file: Path = xodr_dir / f"{str(map_name.name)}.xodr"

        if not xodr_dir.exists():
            self.get_logger().info(message=f"Dir {str(xodr_dir)} does not exist yet. Creating it.")
            xodr_dir.mkdir(parents=True, exist_ok=True)

        if not xodr_file.exists():
            self.get_logger().info(message=f"File {str(xodr_file)} does not exist yet. Creating it.")
            xodr_file.touch()
            with open(file=xodr_file, mode="w") as f:
                f.write(data)
            self.get_logger().info(message=f"Successfully saved map {str(map_name.name)} to {str(xodr_file)} in OpenDrive format.")

    def __save_lanelet2_data(self, map_name: Path, data: str) -> None:

        lanelet2_dir: Path = Path(os.getenv(key="SIMULATION_MAP_FILE_DIR")) / map_name.parent # type: ignore
        lanelet2_file: Path = lanelet2_dir / f"{str(map_name.name)}.osm"

        if not lanelet2_dir.exists():
            self.get_logger().info(message=f"Dir {str(lanelet2_dir)} does not exist yet. Creating it.")
            lanelet2_dir.mkdir(parents=True, exist_ok=True)

        if not lanelet2_file.exists():
            self.get_logger().info(message=f"File {str(lanelet2_file)} does not exist yet. Creating it.")
            lanelet2_file.touch()
            with open(file=lanelet2_file, mode="w") as f:
                f.write(data)
            self.get_logger().info(message=f"Successfully saved map {str(map_name.name)} to {str(lanelet2_file)} in Lanelet2(OSM) format.")
        else:
            self.get_logger().info(message=f"Lanelet2 map file {str(lanelet2_file)} already exists. Skipping creation.")

    def __report_done(self) -> None:
        # Tell the supervisor we’re done
        done = String()
        done.data = self._uuid
        self.done_publisher.publish(done)
        # Stop our own activity - supervisor will drive lifecycle transitions
        if self._timer:
            self._timer.cancel()
            self._timer = None
        self.get_logger().info(message="Reported dont to supervisor.")