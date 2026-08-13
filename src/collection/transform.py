import json
import requests
from datetime import datetime
from datetime import timezone
import time
from pathlib import Path


def grouping_logic(raw_result: list) ->  dict: #Group raw_results into appropriate state observations Grouping by -> station -> lineid -> vehicle_id
    grouped = {}
    for snapshot in raw_result: #Creating the key values for grouping logic (keys arent duplicated, we are filtering for the common values to input observations under)
        station_id = snapshot["station_id"]
        for prediction in snapshot["raw_json"]:
            line_id = prediction.get("lineId")
            vehicle_id = prediction.get("vehicleId")
            platform_name = prediction.get("platformName", "Unknown Platform")

            if not line_id or not vehicle_id:
                continue

            observation = {
                "station_id": station_id,
                "station_name": prediction.get("stationName"),
                "line_id": line_id,
                "line_name": prediction.get("lineName"),
                "vehicle_id": vehicle_id,
                "platform_name": platform_name,
                "current_location": prediction.get("currentLocation"),
                "time_to_station": prediction.get("timeToStation"),
                "destination": prediction.get("destinationName"),
                "towards": prediction.get("towards"),
                "expected_arrival": prediction.get("expectedArrival")
            }

            grouped.setdefault(station_id, {})
            grouped[station_id].setdefault(line_id, {})
            grouped[station_id][line_id].setdefault(vehicle_id, {})
            grouped[station_id][line_id][vehicle_id].setdefault(platform_name, [])

            grouped[station_id][line_id][vehicle_id][platform_name].append(observation)
    return grouped

def parse_datetime(value):
    if not value:
        return None

    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))

    return parsed.isoformat()

def transform_observation(obs: dict) -> dict:
    return {
        "station_id": str(obs.get("station_id")),
        "station_name": obs.get("station_name"),
        "line_id": obs.get("line_id"),
        "line_name": obs.get("line_name"),
        "vehicle_id": str(obs.get("vehicle_id")),
        "platform_name": obs.get("platform_name"),
        "current_location": obs.get("current_location"),
        "time_to_station": int(obs["time_to_station"]) if obs.get("time_to_station") is not None else None,
        "time_to_station_minutes": round(int(obs["time_to_station"]) / 60, 2) if obs.get("time_to_station") is not None else None,
        "destination": obs.get("destination"),
        "towards": obs.get("towards"),
        "expected_arrival": parse_datetime(obs.get("expected_arrival")),
    }



def transform_snapshot(grouped: dict) -> dict:
    if not grouped:
        return {}

    transformed = {}

    for station_id, lines in grouped.items():
        transformed.setdefault(station_id, {})

        for line_id, vehicles in lines.items():
            transformed[station_id].setdefault(line_id, {})

            for vehicle_id, platforms in vehicles.items():
                transformed[station_id][line_id].setdefault(vehicle_id, {})

                for platform_name, observations in platforms.items():
                    transformed[station_id][line_id][vehicle_id].setdefault(platform_name, [])

                    for obs in observations:
                        transformed_obs = transform_observation(obs)
                        transformed[station_id][line_id][vehicle_id][platform_name].append(transformed_obs)
    return transformed

def print_prediction(transformed):
    print(json.dumps(transformed, indent=2, default=str))

    for station_id, lines in transformed.items():
     print(f"\nSTATION: {station_id}")

    for line_id, vehicles in lines.items():
        print(f"  LINE: {line_id}")

        for vehicle_id, platforms in vehicles.items():
            print(f"    VEHICLE: {vehicle_id}")

            for platform_name, observations in platforms.items():
                print(f"      PLATFORM: {platform_name}")

                for obs in observations:
                    print(
                        f"        {obs['current_location']} | "
                        f"{obs['time_to_station']}s | "
                        f"{obs['destination']}"
                    )
