"""
Copyright (C) 2025 Marius Kortmann

This program is free software: you can redistribute it and/or modify it under the terms of the GNU Affero General Public License as published by the Free Software Foundation, either version 3 of the License, or any later version.

This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License along with this program. If not, see <https://www.gnu.org/licenses/>
"""

from pathlib import Path

from rclpy.impl.rcutils_logger import RcutilsLogger

from .xodr_operations import xodr_export_to_json
from .lanelet2_operations import lanelet2_export_to_json

def export_to_json(map_format: str, map_name: Path, data: str, json_dir: Path, json_file: Path, logger: RcutilsLogger, waypoints) -> None:
    match map_format.lower():
        case "lanelet2":
            lanelet2_export_to_json(map_name=map_name, data=data, json_dir=json_dir, json_file=json_file, logger=logger, waypoints=waypoints)
        case "opendrive" | "xodr":
            xodr_export_to_json(map_name=map_name, data=data, json_dir=json_dir, json_file=json_file, logger=logger, waypoints=waypoints)
        case _:
            raise NotImplementedError(f"Map format {map_format} is not supported yet.")

