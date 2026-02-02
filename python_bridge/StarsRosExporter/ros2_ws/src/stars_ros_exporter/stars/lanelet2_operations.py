"""
Copyright (C) 2025 Marius Kortmann

This program is free software: you can redistribute it and/or modify it under the terms of the GNU Affero General Public License as published by the Free Software Foundation, either version 3 of the License, or any later version.

This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License along with this program. If not, see <https://www.gnu.org/licenses/>
"""

import json
import lanelet2
from time import sleep
import math
import orjson
import os

from pathlib import Path
from typing import Dict, List, Set, Tuple, Union

from rclpy.impl.rcutils_logger import RcutilsLogger

from .json_data_classes import (
    Block,
    ContactArea,
    ContactLaneInfo,
    Landmark,
    LandmarkOrientation,
    LandmarkType,
    Lane,
    LaneMidpoint,
    LaneType,
    Location,
    Road,
    Rotation,
    SpeedLimit,
    StaticTrafficLight
)

def lanelet2_export_to_json(map_name: Path, data: str, json_dir: Path, json_file: Path, logger: RcutilsLogger, waypoints) -> None:
    if not json_dir.exists():
        logger.info(message = f"Dir {str(json_dir)} does not exist yet. Creating it.")
        json_dir.mkdir(parents = True, exist_ok = True)
    if not json_file.exists():
        logger.info(message = f"File {str(json_file)} does not exist yet. Creating it.")
        json_file.touch()

    lanelet2_dir: Path = Path(os.getenv(key="SIMULATION_MAP_FILE_DIR")) / map_name.parent # type: ignore
    lanelet2_file: Path = lanelet2_dir / f"{str(map_name.name)}.osm"

    static_data = None

    identity_dir: Path = Path(os.getenv(key="MAP_FILE_DIR")) #type: ignore
    identity_file: Path = identity_dir / "identified_lanelets.json"
    while not identity_file.exists():
        sleep(5)
        logger.info(message="Waiting for identity file to be created...")
    with open(file=identity_file, mode='r') as data_file:
        logger.debug(message=f"Identity file {identity_file} available.")
        json_obj: str = data_file.read()
        identified_lanelets = json.loads(json_obj)

        static_data = build_static_data(identified_lanelets, lanelet2_file, logger)

    with open(file = json_file, mode = "wb") as f:
        f.write(orjson.dumps(static_data, option=orjson.OPT_INDENT_2))

    # We have to replace lane_mid_points because the kotlin deserializer expects lane_midpoints and the python parts adds the extra underscore
    file_content: str = ""
    with open(file = json_file, mode = "r") as f:
        file_content: str = f.read()
    file_content = file_content.replace("lane_mid_points", "lane_midpoints") #type: ignore
    with open(file = json_file, mode = "w") as f:
        f.write(file_content)

    logger.info(message=f"Successfully wrote road blocks to file {json_file}.")

def build_static_data(identified_lanelets, map_file, logger: RcutilsLogger):
    static_data = []

    projector = lanelet2.projection.UtmProjector(lanelet2.io.Origin(0.0, 0.0))
    map = lanelet2.io.load(map_file, projector)

    traffic_rules = lanelet2.traffic_rules.create(
        lanelet2.traffic_rules.Locations.Germany, # currently only Germany is supported
        lanelet2.traffic_rules.Participants.Vehicle
    )

    routing_graph = lanelet2.routing.RoutingGraph(map, traffic_rules)

    lanes = build_lanes(identified_lanelets, map, routing_graph)

    sections = []
    sections_resolved = {}
    junctions = []

    for lanelet in identified_lanelets:
        if lanelet["sectionId"] not in sections:

            sections.append(lanelet["sectionId"])
            sections_resolved[lanelet["sectionId"]] = {str(lanelet["roadId"]): [str(lanelet["laneNumber"])]}

        elif sections_resolved[lanelet["sectionId"]][str(lanelet["roadId"])] is None:

            sections_resolved[lanelet["sectionId"]][str(lanelet["roadId"])] = [str(lanelet["laneNumber"])]

        elif sections_resolved[lanelet["sectionId"]][str(lanelet["roadId"])] is not None and str(lanelet["laneNumber"]) not in sections_resolved[lanelet["sectionId"]][str(lanelet["roadId"])]:

            sections_resolved[lanelet["sectionId"]][str(lanelet["roadId"])].append(str(lanelet["laneNumber"]))

        if lanelet["isJunction"] and lanelet["sectionId"] not in junctions:
            junctions.append(lanelet["sectionId"])

    for key, section in enumerate(sections):
        static_data.append({"id": str(section), "roads": []})
        static_data[key]["id"] = str(section)
        static_data[key]["roads"] = {}

        for road in sections_resolved[str(section)].keys():
            static_data[key]["roads"].append({"road_id": int(road), "is_junction": False, "lanes": []})

            if section in junctions:
                static_data[key]["roads"][-1]["is_junction"] = True

            for lane in sections_resolved[str(section)][str(road)]:

                lane_midpoints = build_lane_midpoints(int(road), int(lane), logger)

                lane_obj = Lane(
                    road_id=int(road),
                    lane_id=int(lane),
                    lane_type=__get_lane_type(lanes[(int(road), int(lane))][0], map),
                    lane_width=3.5,
                    lane_length=calculate_lane_length(lane_midpoints),
                    s=0,
                    predecessor_lanes=lookup_predecessors(lanes[(int(road), int(lane))][0], identified_lanelets, map, routing_graph),
                    successor_lanes=lookup_successors(lanes[(int(road), int(lane))][-1], identified_lanelets, map, routing_graph),
                    intersecting_lanes=[],
                    lane_mid_points=lane_midpoints,
                    speed_limits=[],
                    landmarks=[],
                    contact_areas=[],
                    traffic_lights=[]
                )

                static_data[key]["roads"][-1]["lanes"].append(lane_obj)

    return static_data

def __get_lane_type(lanelet_id, map) -> LaneType:
    lane_type = map.laneltLayer[lanelet_id].attributes["subtype"]
    for e in LaneType:
        if e.name == lane_type.capitalize():
            return e
    return LaneType.Driving

def calculate_lane_length(lane_midpoints):
    n = len(lane_midpoints)
    if n < 2:
        return 0.0

    dist = 0.0
    prev = lane_midpoints[0]["location"]
    px, py = prev["x"], prev["y"]

    for i in range(1, n):
        loc = lane_midpoints[i]["location"]
        x, y = loc["x"], loc["y"]
        dist += math.hypot(x - px, y - py)  # sqrt(dx^2 + dy^2)
        px, py = x, y

    return dist

def build_lane_midpoints(road_id, lane_id, logger: RcutilsLogger):
    lane_mid_points: List[LaneMidpoint] = []
    waypoint_dir: Path = Path(os.getenv(key="MAP_FILE_DIR")) #type: ignore
    waypoint_file: Path = waypoint_dir / "waypoint.json"
    while not waypoint_file.exists():
        sleep(5)
        logger.info(message="Waiting for waypoint file to be created...")
    with open(file=waypoint_file, mode='r') as data_file:
        logger.debug(message=f"Waypoint file {waypoint_file} available, reading it for lane {lane_id} of road {road_id}...")
        json_obj: str = data_file.read()

        data = json.loads(json_obj)
        for elem in data: # should be deserialized to a list of dicts
            if isinstance(elem, dict) and "road_id" in elem and int(elem["road_id"]) == road_id and "lane_id" in elem and int(elem["lane_id"]) == lane_id:
                lane_mid_points.append(
                    LaneMidpoint(
                        road_id=elem["road_id"],
                        lane_id=elem["lane_id"],
                        distance_to_start=elem["distance_to_start"],
                        location=Location(
                            x=elem["location"]["x"],
                            y=elem["location"]["y"],
                            z=elem["location"]["z"]
                        ),
                        rotation=Rotation(
                            pitch=elem["rotation"]["pitch"],
                            yaw=elem["rotation"]["yaw"],
                            roll=elem["rotation"]["roll"]
                        )
                    )
                )
    logger.debug(message=f"Found {len(lane_mid_points)} midpoints for lane {lane_id} of road {road_id}.")
    return lane_mid_points

def lookup_predecessors(lanelet_id, identified_lanelets, map, routing_graph):

    lanelet = map.laneletLayer[lanelet_id]

    predecessor_lanelets = routing_graph.previous(lanelet)

    predeccesors = []
    done = []

    for ll in predecessor_lanelets:
        if ll.id in identified_lanelets.keys():
            if set(identified_lanelets[ll.id]["roadId"], identified_lanelets[ll.id]["laneId"]) not in done:
                predeccesors.append(
                    ContactLaneInfo(
                        road_id=identified_lanelets[ll.id]["roadId"],
                        lane_id=identified_lanelets[ll.id]["laneId"]
                    )
                )
                done.append(set(identified_lanelets[ll.id]["roadId"], identified_lanelets[ll.id]["laneId"]))

    return predeccesors

def lookup_successors(lanelet_id, identified_lanelets, map, routing_graph):

    lanelet = map.laneletLayer[lanelet_id]

    successor_lanelets = routing_graph.following(lanelet)

    successors = []
    done = []

    for ll in successor_lanelets:
        if ll.id in identified_lanelets.keys():
            if set(identified_lanelets[ll.id]["roadId"], identified_lanelets[ll.id]["laneId"]) not in done:
                successors.append(
                    ContactLaneInfo(
                        road_id=identified_lanelets[ll.id]["roadId"],
                        lane_id=identified_lanelets[ll.id]["laneId"]
                    )
                )
                done.append(set(identified_lanelets[ll.id]["roadId"], identified_lanelets[ll.id]["laneId"]))

    return successors


def build_lanes(identified_lanelets, map, routing_graph):
    done = {}
    lanes = {}

    for lanelet in identified_lanelets:
        if lanelet["roadId"] not in done.keys():
            road = lanelet["roadId"]
            laneNo = lanelet["laneNumber"]

            lane_unordered = []
            lane_unordered.append(lanelet)

            for lanelet_2 in identified_lanelets:
                if (
                    lanelet_2["roadId"] == road
                    and lanelet_2["laneNumber"] == laneNo
                    and lanelet_2 != lanelet
                ):
                    lane_unordered.append(lanelet_2)

            done[road] = [laneNo]
            lanes[(road, laneNo)] = order_lanelets(lane_unordered, map, routing_graph)
        elif lanelet["laneNumber"] not in done[lanelet["roadId"]]:
            road = lanelet["roadId"]
            laneNo = lanelet["laneNumber"]

            lane_unordered = []
            lane_unordered.append(lanelet)

            for lanelet_2 in identified_lanelets:
                if (
                    lanelet_2["roadId"] == road
                    and lanelet_2["laneNumber"] == laneNo
                    and lanelet_2 != lanelet
                ):
                    lane_unordered.append(lanelet_2)

            done[road].append(laneNo)
            lanes[(road, laneNo)] = order_lanelets(lane_unordered, map, routing_graph)

    return lanes

def order_lanelets(lane_unordered, map, routing_graph):

    lanelets_by_id = {ll.id: ll for ll in map.laneletLayer}


    def prev_in_set(ll_id):
        ll = lanelets_by_id[ll_id]
        prevs = routing_graph.previous(ll)
        return [p for p in prevs if p.id in lane_unordered]

    def next_in_set(ll_id):
        ll = lanelets_by_id[ll_id]
        nexts = routing_graph.following(ll)
        return [n for n in nexts if n.id in lane_unordered]

    # Heads: lanelets that have no predecessor inside the subset
    heads = [ll for ll in lane_unordered if len(prev_in_set(ll)) == 0]
    if len(heads) != 1:
        raise ValueError(f"Expected exactly 1 head in subset, found {len(heads)}")

    head = heads[0]

    ordered = []
    seen = set()
    cur = head

    while cur is not None:
        if cur.id in seen:
            raise ValueError("Cycle detected in subset")
        seen.add(cur.id)
        ordered.append(cur)

        nxt = next_in_set(cur)
        if len(nxt) == 0:
            cur = None
        elif len(nxt) == 1:
            cur = nxt[0]
        else:
            raise ValueError(
                f"Branch detected at lanelet {cur.id}: {len(nxt)} successors in subset"
            )

    if len(ordered) != len(lane_unordered):
        missing = lane_unordered - {ll.id for ll in ordered}
        raise ValueError(f"Subset is disconnected; missing lanelets: {sorted(missing)}")

    return ordered