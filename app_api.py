from fastapi import FastAPI
import json
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
import requests
from src.collection.request import request_station_data, request_tube_stop_points
from src.collection.transform import grouping_logic, transform_snapshot
from tracking_scopes import TRACKING_SCOPES

app = FastAPI()

LATEST_TRACKER_PATH = Path("data/live/latest_tracker_states.json")
CACHE_STALE_AFTER_SECONDS = 90


def make_scope_slug(station_id: str, line_id: str) -> str:
    return f"{station_id}_{line_id}"


def load_scope_snapshot(
    station_id: str,
    line_id: str
):
    slug = make_scope_slug(station_id, line_id)

    filepath = Path(
        f"data/live/{slug}_latest.json"
    )

    if not filepath.exists():
        return None

    with open(filepath, "r", encoding="utf-8") as file:
        return json.load(file)


'''
--- Root
'''


@app.get("/")
def root():
    return {
        "message": "TfL Tracker API",
        "routes": [
            "/health",
            "/tracker/latest",
            "/tracker/latest/states",
            "/tracker/latest/metadata",
            "/station-board/latest",
            "/arrivals",
            "/stations",
            "/tracking-scopes"
        ]
    }



'''
--- Status Check
'''


@app.get("/health")
def health():
    return {"status" : 'ok'}


'''
--- Status Check
'''
@app.get("/tracking-scopes")
def get_tracking_scopes():
    return {
        "status": "ok",
        "data": TRACKING_SCOPES
    }


@lru_cache(maxsize=1)
def load_station_catalogue() -> list[dict]:
    stations = []

    for stop_point in request_tube_stop_points():
        if stop_point.get("stopType") != "NaptanMetroStation":
            continue

        lines = [
            {
                "line_id": line.get("id", ""),
                "line_name": line.get("name", ""),
            }
            for line in stop_point.get("lines", [])
            if line.get("id") and line.get("name")
        ]
        lines.sort(key=lambda line: line["line_name"].casefold())

        station_id = stop_point.get("naptanId")
        station_name = stop_point.get("commonName")
        if station_id and station_name and lines:
            stations.append({
                "station_id": station_id,
                "station_name": station_name,
                "lines": lines,
            })

    stations.sort(key=lambda station: station["station_name"].casefold())

    if not stations:
        raise ValueError("The TfL station catalogue is empty.")

    return stations


@app.get("/stations")
def get_stations():
    try:
        stations = load_station_catalogue()
    except (requests.RequestException, ValueError, TypeError):
        return {
            "status": "error",
            "message": "Could not load the TfL station catalogue.",
        }

    return {
        "status": "ok",
        "data": stations,
    }

'''
--- Load Helper
'''

def load_latest_tracker_snapshot():
    if not LATEST_TRACKER_PATH.exists():
        return None

    with open(LATEST_TRACKER_PATH, "r", encoding="utf-8") as file:
        return json.load(file)
    

'''
--- Loads Latest Track
'''
@app.get("/tracker/latest")
def get_latest_tracker():
    snapshot = load_latest_tracker_snapshot()

    if snapshot is None:
        return {
            "status": "missing",
            "message": "No latest tracker snapshot found."
        }

    return {
        "status": "ok",
        "data": snapshot
    }


@app.get("/tracker/latest/states")
def get_latest_tracker_states():
    snapshot = load_latest_tracker_snapshot()

    if snapshot is None:
        return {
            "status": "missing",
            "message": "No latest tracker snapshot found."
        }

    states = snapshot.get("tracker_states", [])

    if not isinstance(states, list):
     return {
        "status": "error",
        "message": "tracker_states must be a list."
    }

    return {
        "status": "ok",
        "data": states
    }


@app.get("/tracker/latest/metadata")
def get_latest_tracker_metadata():
    snapshot = load_latest_tracker_snapshot()

    if snapshot is None:
        return {
            "status": "missing",
            "message": "No latest tracker snapshot found."
        }
    
    
    
    metadata = {
        "snapshot_timestamp" : snapshot.get("snapshot_timestamp", "") ,
        "station_id" : snapshot.get("station_id", ""),
        "line_id" : snapshot.get("line_id", "")
    }


    
    return {
        "status": "ok",
        "data": metadata
    }



'''
--- Loads Station Board
'''


def format_station_board(
    snapshot: dict,
    source: str,
    tracked: bool,
    cache_status: str = "not_applicable",
):
    raw_board = snapshot.get("board", [])
    tracker_states = snapshot.get("tracker_states", [])

    if not isinstance(raw_board, list):
        return {
            "status": "error",
            "message": "board must be a list."
        }

    if not isinstance(tracker_states, list):
        return {
            "status": "error",
            "message": "tracker_states must be a list."
        }

    tracker_by_vehicle_id = {
        state.get("vehicle_id"): state
        for state in tracker_states
        if state.get("vehicle_id") is not None
    }

    board = []

    for observation in raw_board:
        vehicle_id = observation.get("vehicle_id")
        tracker_state = tracker_by_vehicle_id.get(vehicle_id)

        board.append({
            "vehicle_id": vehicle_id,
            "platform_name": observation.get("platform_name", "Unknown Platform"),
            "destination": observation.get("destination")
                or observation.get("towards")
                or "Destination unavailable",
            "towards": observation.get("towards", ""),
            "time_to_station_minutes": observation.get("time_to_station_minutes", None),
            "expected_arrival": observation.get("expected_arrival", ""),
            "movement_state": tracker_state.get("movement_state", "untracked")
                if tracker_state
                else "untracked"
        })
    
    grouped_board = {}

    for train in board:
        platform_name = train.get("platform_name", "Unknown Platform")

        if platform_name not in grouped_board:
            grouped_board[platform_name] = []

        grouped_board[platform_name].append(train)

    platforms = []

    for platform_name in sorted(grouped_board.keys()):
        trains = grouped_board[platform_name]

        trains.sort(
            key=lambda train: train.get("time_to_station_minutes")
            if train.get("time_to_station_minutes") is not None
            else float("inf")
        )

        platforms.append({
            "platform_name": platform_name,
            "trains": trains
        })

    return {
        "status": "ok",
        "data": {
            "station_id": snapshot.get("station_id", ""),
            "station_name": snapshot.get("station_name") or snapshot.get("station_id", ""),
            "line_id": snapshot.get("line_id", ""),
            "line_name": snapshot.get("line_name") or snapshot.get("line_id", ""),
            "snapshot_timestamp": snapshot.get("snapshot_timestamp", ""),
            "source": source,
            "tracked": tracked,
            "cache_status": cache_status,
            "updated_at": snapshot.get("snapshot_timestamp", ""),
            "platforms": platforms
        }
    }


def is_tracked_scope(station_id: str, line_id: str) -> bool:
    return any(
        scope["station_id"] == station_id and scope["line_id"] == line_id
        for scope in TRACKING_SCOPES
    )


def is_snapshot_stale(snapshot: dict, now: datetime | None = None) -> bool:
    timestamp = snapshot.get("snapshot_timestamp")
    if not timestamp:
        return True

    try:
        created_at = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return True

    if created_at.tzinfo is None:
        created_at = created_at.replace(tzinfo=timezone.utc)

    current_time = now or datetime.now(timezone.utc)
    return (current_time - created_at).total_seconds() > CACHE_STALE_AFTER_SECONDS


def build_live_snapshot(station_id: str, line_id: str):
    raw_result = request_station_data(station_id)
    if raw_result is None:
        return None

    transformed = transform_snapshot(grouping_logic([raw_result]))
    line = transformed.get(station_id, {}).get(line_id, {})
    board = [
        observation
        for platforms in line.values()
        for observations in platforms.values()
        for observation in observations
    ]
    board.sort(
        key=lambda observation: observation.get("time_to_station")
        if observation.get("time_to_station") is not None
        else float("inf")
    )

    first = board[0] if board else {}
    return {
        "snapshot_timestamp": datetime.now(timezone.utc).isoformat(),
        "station_id": station_id,
        "station_name": first.get("station_name", station_id),
        "line_id": line_id,
        "line_name": first.get("line_name", line_id),
        "board": board,
        "tracker_states": [],
    }


@app.get("/arrivals")
def get_arrivals(station_id: str, line_id: str):
    tracked = is_tracked_scope(station_id, line_id)
    cached_snapshot = load_scope_snapshot(station_id, line_id) if tracked else None

    if tracked and cached_snapshot is not None and not is_snapshot_stale(cached_snapshot):
        return format_station_board(
            cached_snapshot,
            source="cache",
            tracked=True,
            cache_status="fresh",
        )

    snapshot = build_live_snapshot(station_id, line_id)

    if snapshot is None:
        return {
            "status": "error",
            "message": f"Could not load current arrivals for {station_id}_{line_id}.",
        }

    return format_station_board(
        snapshot,
        source="live",
        tracked=tracked,
        cache_status="stale_fallback" if tracked else "not_applicable",
    )


@app.get("/station-board/latest")
def get_latest_station_board(station_id: str, line_id: str):
    snapshot = load_scope_snapshot(station_id, line_id)
    if snapshot is None:
        return {
            "status": "missing",
            "message": f"No latest tracker snapshot found for {station_id}_{line_id}."
        }

    return format_station_board(snapshot, source="cache", tracked=True)


'''
--- Loads Station Board (Query-User-Based)
'''
