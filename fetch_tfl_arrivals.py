import json
import requests
from datetime import datetime
from datetime import timezone
import time
from pathlib import Path
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from tracking_scopes import TRACKING_SCOPES



'''
------------Data Ingestion---------------
'''

def request_station_data(station_id: str) -> dict | None:
    try:
        response = requests.get(
            f"https://api.tfl.gov.uk/StopPoint/{station_id}/Arrivals",
            timeout=10
        )

        if response.status_code == 200:
            return {
                "station_id": station_id,
                "status_code": response.status_code,
                "raw_json": response.json()
            }

        print(f"Request failed for {station_id}: {response.status_code}")
        return None

    except requests.RequestException as error:
        print(f"Request failed for {station_id}: {error}")
        return None

def requests_data(station_ids: list[str]) -> list[dict]:
    raw_result = []

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [
            executor.submit(request_station_data, station_id)
            for station_id in station_ids
        ]

        for future in as_completed(futures):
            result = future.result()

            if result is not None:
                raw_result.append(result)

    return raw_result


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

'''
------------Snapshot State Persistence (Load & Save)---------------
'''

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

'''
------------Scope-Slug Helper---------------
'''

def make_scope_slug(station_id: str, line_id: str) -> str:
    return f"{station_id}_{line_id}"

'''
------------Snapshot State Persistence (Load & Save)---------------
'''

#Persistence snapshot, keep this seperate from the request API functions
def save_snapshot(snapshot_data : dict): #saves one snapshot at a time
    #create time_stamp
    time_now = datetime.now(timezone.utc).isoformat()
    meta_data = {
        'created_at': time_now,
        'requested_station_ids' : station_ids, #stores the station_id's requested, this only works because station_ids is a global variable, so later implementation when we begin to modularise needs to change this.
        'requested_station_count' : len(station_ids) #stores the number of station_id's requested
    }
    
    #Wrap the metadata around data (notice, we are still preserving the data)
    snapshot = {
        "metadata": meta_data,
        "data": snapshot_data
    }


    #Creating filepath
    PROCESSED_DIR = Path("data/processed/snapshots") #Creates a path object relative to where the script is running.
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True) #Creates the path relative to the script, if data/processed/snapshots does not exist.

    #So that we can save it on a windows system.
    slug_time = time_now.replace(":", "-")
    filename = f"snapshot_{slug_time}.json"
    filepath = PROCESSED_DIR / filename


    with open(filepath, 'w') as file: #Opens at filepath as write mode, creates the file variable, file represents the feeding material, the interface that allows data such as json.dump to be written into file.
        json.dump(snapshot, file, indent=4)

    print("Save Successful")


def load_snapshot(file_path): #Loading logic should be simple, this stage it only loads whats asked, the deciding what file to load will come up later
    filepath = Path(file_path)

    with open(filepath, 'r', encoding="utf-8") as file:
        snapshot = json.load(file)

    return snapshot 


def list_snapshots(): #Creates a list of filenames in our snapshot folder, sets up for future queue stacks.
    SNAPSHOT_DIR = Path("data/processed/snapshots")
    return list(SNAPSHOT_DIR.glob("*.json")) #Important to note, they return Windowpaths, which comes with different attributes, like .name, .stem etc.


def save_latest_tracker_snapshot(snapshot: dict):
    station_id = snapshot["station_id"]
    line_id = snapshot["line_id"]

    slug = make_scope_slug(station_id, line_id)
    filepath = Path(f"data/live/{slug}_latest.json")

    save_json(snapshot, filepath)



"""
From this stage, this is mostly on state observations, and creating the logic required for live state tracking
"""


'''
------------Comparison Layer---------------
'''

def raw_comparison(old_obs: dict, new_obs: dict) -> dict:
    ''' 
    After being processed by platform logic (picks the closest platform to where the vehicle currently is)
    This should simply calculate the raw distance of the variables
    time_to_station : int
    time_to_station_minutes : float

    '''

    required_fields = ['time_to_station', 'time_to_station_minutes']

    for field in required_fields:
        if old_obs.get(field) is None or new_obs.get(field) is None:
            raise ValueError(f"Missing required field: {field}") #Should look into how outer functions react to missing fields like this
        
    try:
        time_to_station_old = int(old_obs.get('time_to_station'))
        time_to_station_new = int(new_obs.get('time_to_station'))

        time_to_station_minutes_old = float(old_obs.get('time_to_station_minutes'))
        time_to_station_minutes_new = float(new_obs.get('time_to_station_minutes'))
    except (TypeError, ValueError) as error:
        raise ValueError("Invalid time field in observation") from error 


    #Calculations if + = delayed, further if - = moving if 0 = not moving
    calculated_time_to_station = time_to_station_new - time_to_station_old
    calculated_time_to_station_minutes = time_to_station_minutes_new - time_to_station_minutes_old

    calculated_time = {
        "station_name": new_obs.get("station_name"),
        "platform_name": new_obs.get("platform_name"),
        'vehicle_id' : new_obs.get('vehicle_id'),
        'line_id': new_obs.get('line_id'),
        'station_id' : new_obs.get('station_id'),
        "destination": new_obs.get("destination"),
        "towards": new_obs.get("towards"),

        'time_to_station_old': time_to_station_old,
        'time_to_station_new': time_to_station_new,
        'delta_time_to_station' : calculated_time_to_station,

        'time_to_station_minutes_old' : time_to_station_minutes_old,
        'time_to_station_minutes_new' : time_to_station_minutes_new,
        'delta_time_to_station_minutes' : calculated_time_to_station_minutes,
        #I would add a field for created_at for snapshot values, but maybe could be done by outer function with access to metadata.

        "old_current_location": old_obs.get("current_location"),
        "new_current_location": new_obs.get("current_location"),
        "current_location_changed": old_obs.get("current_location") != new_obs.get("current_location"),
        "platform_changed": old_obs.get("platform_name") != new_obs.get("platform_name"),
        "destination_changed": old_obs.get("destination") != new_obs.get("destination"),
        "expected_arrival_changed": old_obs.get("expected_arrival") != new_obs.get("expected_arrival"),
    }


    #Need a note somewhere to show how calculated times shoould be interpreted
    return calculated_time


def classify_observations(comparison: dict) -> dict:
    '''
    raw_comparison -> classify_observations
    States should be interpreted from the raw_comparisons, we should be able to interpret the states of the train from raw comparisons
    My plan is to list this out as a dictionary, containing variables such as state : moving (string), closest_to : Finchley Road (string)
    etc. Later implementations could also consider using enums to look up from, so that the strings wouldn't always have to be consistently recreated as a string

    Edge cases : realistically, this shouldnt fail because raw_comparison already has some edge case implementations

    Return schema {
    
    #Identification
    vehicleid : (id : int),
    line_id : (line_id : int),
    station_id : (station_id : int),

    #Classification
    state_transition : (state : string),
    closest_to : (station_name : string),
    time_to_station : (seconds : int),
    
    }

        Classify a raw comparison into a prediction transition.

    Assumption:
    delta_time_to_station = new_time_to_station - old_time_to_station

    Therefore:
    positive delta -> prediction moved closer to this station
    negative delta -> prediction moved further from this station
    zero delta     -> prediction unchanged

    '''

    if comparison is None:
        raise ValueError("comparison is missing")

    delta = comparison.get("delta_time_to_station")

    if delta is None:
        prediction_transition = "unknown"
        reason = "missing delta_time_to_station"
    elif delta < 0:
        prediction_transition = "closer_to_station"
        reason = "new time_to_station is lower than old time_to_station"
    elif delta > 0:
        prediction_transition = "further_from_station"
        reason = "new time_to_station is higher than old time_to_station"
    else:
        prediction_transition = "unchanged_to_station"
        reason = "time_to_station did not change"

    return {
        "vehicle_id": comparison.get("vehicle_id"),
        "line_id": comparison.get("line_id"),
        "station_id": comparison.get("station_id"),
        "station_name": comparison.get("station_name"),
        "platform_name": comparison.get("platform_name"),
        "destination": comparison.get("destination"),
        "towards": comparison.get("towards"),

        "prediction_transition": prediction_transition,
        "delta_time_to_station": delta,
        "time_to_station_old": comparison.get("time_to_station_old"),
        "time_to_station_new": comparison.get("time_to_station_new"),

        "reason": reason,
    }



def compare_vehicle_observations(old_obs: dict, new_obs: dict) -> dict:
    comparison = raw_comparison(old_obs, new_obs)
    classification = classify_observations(comparison)

    return {
        "comparison": comparison,
        "classification": classification,
    }


'''
------------Snapshot Navigation Layer---------------
'''

#Note, snapshots need to be directly loaded into snapshots[data]

def list_stations(snapshot: dict) -> list[str]:
    #I wonder if this needs to get past the meta data structure 
    return list(snapshot.keys())

def get_station(snapshot: dict, station_id: str) -> dict:
    return snapshot.get(station_id, {})

def list_lines(snapshot: dict, station_id: str) -> list[str]:
    station = get_station(snapshot, station_id)
    return list(station.keys())

def get_line(snapshot: dict, station_id: str, line_id: str) -> dict:
    station = get_station(snapshot, station_id)
    return station.get(line_id, {})

def get_vehicle_context(snapshot: dict, station_id: str, line_id: str, vehicle_id: str) -> dict:
    line = get_line(snapshot, station_id, line_id)
    return line.get(vehicle_id, {})

def list_vehicles(snapshot: dict, station_id: str, line_id: str) -> list[str]:
    line = get_line(snapshot, station_id, line_id)
    return list(line.keys())

def list_platforms(snapshot: dict, station_id: str, line_id: str, vehicle_id: str) -> list[str]:
    vehicle_context = get_vehicle_context(snapshot, station_id, line_id, vehicle_id)
    return list(vehicle_context.keys())

def get_platform_observations(snapshot: dict, station_id: str, line_id: str, vehicle_id: str, platform_name: str) -> list[dict]:
    vehicle_context = get_vehicle_context(snapshot, station_id, line_id, vehicle_id)
    return vehicle_context.get(platform_name, [])

def vehicle_exists(snapshot, station_id, line_id, vehicle_id) -> bool:
    return vehicle_id in list_vehicles(snapshot, station_id, line_id)

'''
------------Selection layer---------------
'''

def collect_vehicle_observations(snapshot: dict, station_id: str, line_id: str, vehicle_id: int) -> list:
    candidates = []
    platform_names = list_platforms(snapshot, station_id, line_id, vehicle_id)
    for observations in platform_names:
        candidates.extend(get_platform_observations(
    snapshot,
    station_id,
    line_id,
    vehicle_id,
    observations
))

    return candidates


def select_closest_observation_for_station(candidates: list) -> dict:
    selection = []

    for observation in candidates:
        if is_valid_tracking_observation(observation):
            selection.append(observation)

    if selection == []:
        return None

    result = selection[0]
    smallest_time_to_station = selection[0].get("time_to_station")

    for item in selection:
        if smallest_time_to_station > item.get("time_to_station"):
            result = item
            smallest_time_to_station = item.get("time_to_station")

    return {
        "selected_observation": result,
        "selection_policy": "smallest_valid_eta_excluding_ambiguous_observations",
        "candidate_count": len(candidates),
        "valid_candidate_count": len(selection),
        "reason": "Selected lowest ETA after filtering invalid or ambiguous observations."
    }


'''
------------Context Selection Layer--------------
'''

#Layer that feeds context into the Selection layer

def get_available_contexts(snapshot: dict) -> list[dict]:
    """
    Return all station/line/vehicle combinations available in a snapshot.
    """
    contexts = []

    for station_id, lines in snapshot.items():
        for line_id, vehicles in lines.items():
            for vehicle_id in vehicles.keys():
                contexts.append({
                    "station_id": station_id,
                    "line_id": line_id,
                    "vehicle_id": vehicle_id,
                })

    return contexts


def context_exists(snapshot: dict, context: dict) -> bool:
    """
    Check whether this station/line/vehicle context exists in a snapshot.
    """
    
    station_id = context["station_id"]
    line_id = context["line_id"]
    vehicle_id = context["vehicle_id"]


    return vehicle_exists(snapshot, station_id, line_id, vehicle_id)

def is_valid_tracking_observation(obs: dict) -> bool:
    if obs.get("time_to_station") is None:
        return False

    if obs.get("destination") is None:
        return False

    if obs.get("towards") == "Check Front of Train":
        return False

    return True

def select_tracking_context(old_snapshot: dict, new_snapshot: dict) -> dict | None:
    """
    Select one context that exists in both snapshots and has valid observations.

    Policy v2:
    first_shared_context_with_valid_observations
    """

    old_snapshot_contexts = get_available_contexts(old_snapshot)
    new_snapshot_contexts = get_available_contexts(new_snapshot)

    for context in old_snapshot_contexts:
        if context not in new_snapshot_contexts:
            continue

        station_id = context["station_id"]
        line_id = context["line_id"]
        vehicle_id = context["vehicle_id"]

        old_candidates = collect_vehicle_observations(
            old_snapshot, station_id, line_id, vehicle_id
        )

        new_candidates = collect_vehicle_observations(
            new_snapshot, station_id, line_id, vehicle_id
        )

        old_valid = [
            obs for obs in old_candidates
            if is_valid_tracking_observation(obs)
        ]

        new_valid = [
            obs for obs in new_candidates
            if is_valid_tracking_observation(obs)
        ]

        if old_valid and new_valid:
            return {
                "selected_policy": "v2",
                "selected_context": context,
                "reason": "Selected first shared context with valid observations in both snapshots.",
                "old_valid_candidate_count": len(old_valid),
                "new_valid_candidate_count": len(new_valid),
            }

    return None





'''
------------Identity continuity helper---------------
'''

def evaluate_identity_continuity(old_obs: dict, new_obs: dict) -> dict:

    evidence = []
    confidence = 0

    old_vehicle_id = old_obs.get("vehicle_id")
    new_vehicle_id = new_obs.get("vehicle_id")

    if not old_vehicle_id or not new_vehicle_id:
        return {
        "identity_state": "unknown",
        "confidence": 0,
        "evidence": [f"vehicle does not exist in {old_obs} or {new_obs}"]
    }

    if old_vehicle_id != new_vehicle_id:
     return {
        "identity_state": "different_train",
        "confidence": 0,
        "evidence": [
            f"vehicle id changed from {old_vehicle_id} to {new_vehicle_id}"
        ]
    }


    if old_vehicle_id == new_vehicle_id:
        confidence += 50
        evidence.append("vehicle_id matches")
    

    if old_obs.get("line_id") == new_obs.get("line_id"):
        confidence += 15
        evidence.append("line_id matches")

    if old_obs.get("station_id") == new_obs.get("station_id"):
        confidence += 15
        evidence.append("station_id matches")

    if old_obs.get("destination") == new_obs.get("destination"):
        confidence += 10
        evidence.append("destination matches")

    if old_obs.get("towards") == new_obs.get("towards"):
        confidence += 5
        evidence.append("towards matches")

    old_eta = old_obs.get("time_to_station")
    new_eta = new_obs.get("time_to_station")

    if old_eta is not None and new_eta is not None:
        if new_eta <= old_eta:
            confidence += 10
            evidence.append("ETA decreased or stayed plausible")
        else:
            evidence.append("ETA increased")


    identity_state = "same_train" if confidence >= 70 else "unknown"


    return {
        "identity_state": identity_state,
        "confidence": confidence,
        "evidence": evidence
    }



'''
------------Snapshot-to-Snapshot Tracking Layer---------------
'''



def compare_context_between_snapshots(
    old_snapshot: dict,
    new_snapshot: dict,
    context: dict
) -> dict:
    """
    Compare one selected context across two snapshots.

    Steps:
    - collect old candidates
    - collect new candidates
    - select old representative observation
    - select new representative observation
    - compare selected observations
    - classify result
    """

    #context should include station_id, vehicle_id and line_id, maybe even use the previous if_context_exist function we made earlier here


    station_id = context['station_id']
    vehicle_id = context['vehicle_id']
    line_id = context['line_id']


    
    old_snapshot_observations = collect_vehicle_observations(old_snapshot, station_id, line_id, vehicle_id)
    new_snapshot_observations = collect_vehicle_observations(new_snapshot, station_id, line_id, vehicle_id)


    old_selected_observation = select_closest_observation_for_station(old_snapshot_observations)
    new_selected_observation = select_closest_observation_for_station(new_snapshot_observations)

    if old_selected_observation is None or new_selected_observation is None:
        return {
            'status' : 'missing_selected_observation',
            'context' : context,
            'old_selected_observation' : old_selected_observation,
            'new_selected_observation' : new_selected_observation
        }
    
    identity_result = evaluate_identity_continuity(
    old_selected_observation["selected_observation"],
    new_selected_observation["selected_observation"]
    )

    if identity_result["identity_state"] != "same_train":
     return {
        "status": "identity_not_confirmed",
        "context": context,
        "identity_result": identity_result,
        "old_selected_observation": old_selected_observation,
        "new_selected_observation": new_selected_observation,
        "comparison": None,
    }

    comparison = compare_vehicle_observations(old_selected_observation['selected_observation'], new_selected_observation['selected_observation'])


    return {
        'status': 'completed',
        'context' : context,
        'old_selected_observation' : old_selected_observation,
        'new_selected_observation' : new_selected_observation,
        'comparison' : comparison
    }


def run_tracking_prototype(old_snapshot: dict, new_snapshot: dict) -> dict:
    """
    Full v1 prototype:
    - select shared context
    - compare that context
    - return result
    """
    

    shared_context = select_tracking_context(old_snapshot, new_snapshot)

    if shared_context is None:
        return {
            'status' : 'no_shared_context'
        }
    
    context = shared_context['selected_context']

    comparison_result = compare_context_between_snapshots(old_snapshot, new_snapshot, context)

    return{
        'context_result' : shared_context,
        'comparison_result' : comparison_result
    }
    

'''
------------Comparison Classification---------------
'''

'''
------------State History Layer---------------
'''




def create_comparison_history(max_size=10):
    history = deque(maxlen = max_size)
    return history


def extract_classification_from_tracking_result(tracking_result: dict) -> dict | None:
    #given an item from the classification, should just extract the classification type

    return {
        'context_result' : tracking_result['context_result'],
        'classification_result' : tracking_result['comparison_result']['comparison']['classification']
    }


def update_comparison_history(history, tracking_result: dict):
        history_item = extract_classification_from_tracking_result(
        tracking_result
    )  #This extends to a prebuilt function to extract an item

        history.append(history_item)

        return history
    


'''
------------Summary / Interpretation Layer---------------
'''

def count_prediction_transitions(history) -> dict:
    #counts the number of prediction transitions, for each state

    frequency = {}

    for item in history:
        transition = item['classification_result']['prediction_transition']

        frequency[transition] = frequency.setdefault(transition, 0) + 1
    
    return frequency 


def get_dominant_transition(counts: dict) -> str:
    
    return max(counts, key=counts.get)


def summarize_comparison_history(history) -> dict:
    if not history: #Checks for if history is empty or not
        return {
            "status": "empty_history",
            "dominant_transition": None,
            "transition_counts": {},
            "sample_size": 0
        }

    transition_counts = count_prediction_transitions(history)
    dominant_transition = get_dominant_transition(transition_counts)
    sample_size = sum(transition_counts.values())

    return {
        "status": "completed",
        "dominant_transition": dominant_transition,
        "transition_counts": transition_counts,
        "sample_size": sample_size
    }


def build_tracker_state(context: dict, history) -> dict:
    summary = summarize_comparison_history(history)

    latest_item = history[-1] if history else None
    latest_classification = (
        latest_item.get("classification_result")
        if latest_item
        else {}
    )

    return {
    "line_id": context.get("line_id"),
    "vehicle_id": context.get("vehicle_id"),
    "station_id": context.get("station_id"),

    "platform_name": latest_classification.get("platform_name"),
    "station_name": latest_classification.get("station_name"),

    "destination": latest_classification.get("destination"),
    "towards": latest_classification.get("towards"),

    "movement_state": summary["dominant_transition"],

    "latest_eta_seconds": latest_classification.get("time_to_station_new"),
    "latest_eta_minutes": (
        round(latest_classification.get("time_to_station_new") / 60, 2)
        if latest_classification.get("time_to_station_new") is not None
        else None
    ),

    "history_sample_size": summary["sample_size"],
    "transition_counts": summary["transition_counts"],
    "summary_status": summary["status"]
}


'''
------------Context-Scope Selection/Observations/History For Multi-Vehicles---------------
'''


def get_contexts_for_scope(snapshot: dict, station_id: str, line_id: str) -> list[dict]:
    
    #User selects station_id and line_id
    #Select latest snapshot, to reflect what is available to track
    #Returns all vehicle_id's within the wanted context.

    context = []

    vehicles = list_vehicles(snapshot, station_id, line_id)

    for vehicle in vehicles:
        context.append(
    {"station_id": station_id, 
     "line_id": line_id, 
     "vehicle_id": vehicle}
    )

    return context
    
def make_context_key(context: dict) -> str:
    return (
        f"{context['station_id']}:"
        f"{context['line_id']}:"
        f"{context['vehicle_id']}"
    )

def compare_all_contexts_between_snapshots(
    old_snapshot: dict,
    new_snapshot: dict,
    contexts: list[dict]
) -> list[dict]:

    results = []

    for context in contexts:
        result = compare_context_between_snapshots(
            old_snapshot,
            new_snapshot,
            context
        )

        results.append(result)

    return results


def update_histories_for_scope(histories: dict, tracking_results: list[dict]) -> dict:
    for result in tracking_results:
        if result.get("status") != "completed":
            continue

        context = result["context"]
        key = make_context_key(context)

        if key not in histories:
            histories[key] = create_comparison_history()

        histories[key].append({
            "context": context,
            "classification_result": result["comparison"]["classification"]
        })

    return histories


def build_tracker_states_for_scope(histories: dict) -> list[dict]:
    tracker_states = []

    for key, history in histories.items():
        if not history:
            continue

        latest_item = history[-1]
        context = latest_item["context"]

        tracker_state = build_tracker_state(
            context,
            history
        )

        tracker_states.append(tracker_state)

    return tracker_states


def run_scope_tracking_cycle(
    old_snapshot: dict,
    new_snapshot: dict,
    station_id: str,
    line_id: str,
    histories: dict
) -> dict:
    
    contexts = get_contexts_for_scope(
        new_snapshot,
        station_id,
        line_id
    )

    tracking_results = compare_all_contexts_between_snapshots(
        old_snapshot,
        new_snapshot,
        contexts
    )

    histories = update_histories_for_scope(
        histories,
        tracking_results
    )

    tracker_states = build_tracker_states_for_scope(
        histories
    )

    current_board = build_current_board_for_scope(
    new_snapshot,
    station_id,
    line_id
)

    return {
    "station_id": station_id,
    "line_id": line_id,
    "contexts": contexts,
    "tracking_results": tracking_results,
    "histories": histories,
    "tracker_states": tracker_states,
    "current_board": current_board
}

'''
------------Presentation Layer---------------
'''

def group_tracker_states_by_platform(tracker_states: list[dict]) -> dict[str, list[dict]]:
    grouped_platforms = {}

    for state in tracker_states:
        platform = state["platform_name"]

        if platform not in grouped_platforms:
            grouped_platforms[platform] = []

        grouped_platforms[platform].append(state)

    return grouped_platforms

def print_tracker_states(tracker_states: list[dict]):

    grouped_platforms = group_tracker_states_by_platform(tracker_states)

    print("\n" + "=" * 70)
    print("LIVE TRACKER")
    print("=" * 70)

    for platform, states in grouped_platforms.items():

        states.sort(
            key=lambda x: x["latest_eta_seconds"]
        )

        print(f"\n{platform}")
        print("-" * 70)

        for state in states:
            print(
                f"""
Vehicle:      {state['vehicle_id']}
Direction:    {state['towards']}
Destination:  {state['destination']}

ETA:          {state['latest_eta_minutes']} min
State:        {state['movement_state']}
"""
            )

        print("-" * 70)

#Live Testing - Here

def run_live_scope_tracker(
    station_ids: str,
    line_id: str,
    poll_interval: int = 30
):
    histories = {}

    old_snapshot = transform_snapshot(
        grouping_logic(
            requests_data(station_ids)
        )
    )

    try:
        while True:

            time.sleep(poll_interval)

            new_snapshot = transform_snapshot(
                grouping_logic(
                    requests_data(station_ids)
                )
            )

            result = run_scope_tracking_cycle(
                old_snapshot,
                new_snapshot,
                station_id,
                line_id,
                histories
            )

            print_tracker_states(result["tracker_states"])

            snapshot = build_tracker_snapshot(
            station_id,
            line_id,
            result["tracker_states"]
        )

            save_latest_tracker_snapshot(snapshot)
            save_historical_tracker_snapshot(snapshot)

            old_snapshot = new_snapshot

    except KeyboardInterrupt:
        print("Tracker stopped.")

def run_live_multi_scope_tracker(
    tracking_scopes: list[dict],
    poll_interval: int = 30
):
    histories_by_scope = {}

    station_ids = list({
        scope["station_id"]
        for scope in tracking_scopes
    })

    old_snapshot = transform_snapshot(
        grouping_logic(
            requests_data(station_ids)
        )
    )

    try:
        while True:
            time.sleep(poll_interval)

            new_snapshot = transform_snapshot(
                grouping_logic(
                    requests_data(station_ids)
                )
            )

            for scope in tracking_scopes:
                station_id = scope["station_id"]
                line_id = scope["line_id"]

                scope_key = make_scope_slug(station_id, line_id)

                if scope_key not in histories_by_scope:
                    histories_by_scope[scope_key] = {}

                result = run_scope_tracking_cycle(
                    old_snapshot,
                    new_snapshot,
                    station_id,
                    line_id,
                    histories_by_scope[scope_key]
                )

                snapshot = build_tracker_snapshot(
                station_id,
                line_id,
                result["tracker_states"],
                result["current_board"]
            )

                save_latest_tracker_snapshot(snapshot)
                save_historical_tracker_snapshot(snapshot)

                print(f"Saved latest tracker snapshot for {scope_key}")

            old_snapshot = new_snapshot

    except KeyboardInterrupt:
        print("Multi-scope tracker stopped.")

'''
------------Serialisation---------------
'''

def tracker_state_to_dict(tracker_state):
    return {
        "vehicle_id": tracker_state.get("vehicle_id"),
        "station_id": tracker_state.get("station_id"),
        "line_id": tracker_state.get("line_id"),
        "platform_name": tracker_state.get("platform_name"),
        "station_name": tracker_state.get("station_name"),
        "destination": tracker_state.get("destination"),
        "towards": tracker_state.get("towards"),
        "time_to_station": tracker_state.get("latest_eta_seconds"),
        "time_to_station_minutes": tracker_state.get("latest_eta_minutes"),
        "movement_state": tracker_state.get("movement_state"),
    }

    
def build_tracker_snapshot(
    station_id,
    line_id,
    tracker_states,
    current_board
):
    return {
        "snapshot_timestamp": datetime.now(timezone.utc).isoformat(),
        "station_id": station_id,
        "line_id": line_id,
        "board": current_board,
        "tracker_states": [
            tracker_state_to_dict(state)
            for state in tracker_states
        ]
    }


def save_json(data: dict, filepath: Path):
    filepath.parent.mkdir(parents=True, exist_ok=True)

    with open(filepath, "w", encoding="utf-8") as file:
        json.dump(data, file, indent=4)


def save_historical_tracker_snapshot(snapshot: dict):
    timestamp = snapshot["snapshot_timestamp"].replace(":", "-")
    filepath = Path(f"data/history/snapshots/snapshot_{timestamp}.json")
    save_json(snapshot, filepath)







'''
------------Testing---------------
'''



def test_real_vehicle_430_aldgate_metropolitan():
    old_obs = {
        "vehicle_id": "430",
        "line_id": "metropolitan",
        "station_id": "940GZZLUALD",
        "station_name": "Aldgate Underground Station",
        "platform_name": "Northbound - Platform 3",
        "current_location": "At Kings Cross St. Pancras Platform 2",
        "time_to_station": 579,
        "time_to_station_minutes": 9.65,
    }

    new_obs = {
        "vehicle_id": "430",
        "line_id": "metropolitan",
        "station_id": "940GZZLUALD",
        "station_name": "Aldgate Underground Station",
        "platform_name": "Northbound - Platform 3",
        "current_location": "At Platform",
        "time_to_station": 31,
        "time_to_station_minutes": 0.52,
    }

    comparisons_vehicle = compare_vehicle_observations(old_obs, new_obs)

    print("\nREAL TEST — Aldgate Metropolitan Vehicle 430")
    print(comparisons_vehicle['comparison'])
    print(comparisons_vehicle['classification'])

def test_snapshot_access_layer(snapshot: dict) -> None:
    print("\nTEST — Snapshot Access Layer")

    stations = list_stations(snapshot)
    assert stations, "Expected at least one station"
    print("Stations:", stations)

    station_id = stations[0]

    lines = list_lines(snapshot, station_id)
    assert lines, f"Expected at least one line for station {station_id}"
    print("Lines:", lines)

    line_id = lines[0]

    vehicles = list_vehicles(snapshot, station_id, line_id)
    assert vehicles, f"Expected at least one vehicle for {station_id} / {line_id}"
    print("Vehicles:", vehicles)

    vehicle_id = vehicles[0]
    assert vehicle_exists(snapshot, station_id, line_id, vehicle_id)

    platforms = list_platforms(snapshot, station_id, line_id, vehicle_id)
    assert platforms, f"Expected at least one platform for vehicle {vehicle_id}"
    print("Platforms:", platforms)

    platform_name = platforms[0]

    observations = get_platform_observations(
        snapshot,
        station_id,
        line_id,
        vehicle_id,
        platform_name
    )

    assert isinstance(observations, list), "Expected observations to be a list"
    print("Observation count:", len(observations))

    if observations:
        print("First observation:")
        print(observations[0])

    print("Access layer traversal passed.")


def test_snapshot_access_layer_invalid_cases(snapshot: dict) -> None:
    print("\nTEST — Invalid Access Cases")

    assert get_station(snapshot, "fake_station") == {}
    assert list_lines(snapshot, "fake_station") == []
    assert get_line(snapshot, "fake_station", "fake_line") == {}
    assert list_vehicles(snapshot, "fake_station", "fake_line") == []
    assert get_vehicle_context(snapshot, "fake_station", "fake_line", "fake_vehicle") == {}
    assert list_platforms(snapshot, "fake_station", "fake_line", "fake_vehicle") == []
    assert get_platform_observations(
        snapshot,
        "fake_station",
        "fake_line",
        "fake_vehicle",
        "fake_platform"
    ) == []

    print("Invalid access cases passed.")

def test_selection_layer(snapshot: dict) -> None:
    print("\nTEST — Selection Layer")

    station_id = list_stations(snapshot)[0]
    line_id = list_lines(snapshot, station_id)[0]
    vehicle_id = list_vehicles(snapshot, station_id, line_id)[0]

    print("Selected test context:")
    print("Station:", station_id)
    print("Line:", line_id)
    print("Vehicle:", vehicle_id)

    candidates = collect_vehicle_observations(
        snapshot,
        station_id,
        line_id,
        vehicle_id
    )

    print("Candidate count:", len(candidates))
    print("Candidates:", candidates)

    assert isinstance(candidates, list)
    assert candidates, "Expected candidates to exist"
    assert isinstance(candidates[0], dict), "Candidates should be a flat list of dictionaries"

    selection = select_closest_observation_for_station(candidates)

    print("Selection result:", selection)

    assert selection is not None

    selected_obs = selection["selected_observation"]

    valid_candidates = [
    obs for obs in candidates
    if is_valid_tracking_observation(obs)
]

    assert selected_obs["time_to_station"] == min(
    obs["time_to_station"]
    for obs in valid_candidates
)

    print("Selection layer passed.")

def test_identity_continuity_same_train():
    print("\nTEST — Identity Continuity Same Train")

    old_obs = {
        "vehicle_id": "430",
        "line_id": "metropolitan",
        "station_id": "940GZZLUALD",
        "destination": "Aldgate",
        "towards": "Aldgate",
        "time_to_station": 500,
    }

    new_obs = {
        "vehicle_id": "430",
        "line_id": "metropolitan",
        "station_id": "940GZZLUALD",
        "destination": "Aldgate",
        "towards": "Aldgate",
        "time_to_station": 300,
    }

    result = evaluate_identity_continuity(old_obs, new_obs)

    print(result)

    assert result["identity_state"] == "same_train"
    assert result["confidence"] >= 70
    assert "vehicle_id matches" in result["evidence"]

    print("Identity continuity same train passed.")


def test_identity_continuity_different_train():
    print("\nTEST — Identity Continuity Different Train")

    old_obs = {
        "vehicle_id": "430",
        "line_id": "metropolitan",
        "station_id": "940GZZLUALD",
        "destination": "Aldgate",
        "towards": "Aldgate",
        "time_to_station": 500,
    }

    new_obs = {
        "vehicle_id": "999",
        "line_id": "metropolitan",
        "station_id": "940GZZLUALD",
        "destination": "Aldgate",
        "towards": "Aldgate",
        "time_to_station": 300,
    }

    result = evaluate_identity_continuity(old_obs, new_obs)

    print(result)

    assert result["identity_state"] == "different_train"
    assert result["confidence"] == 0

    print("Identity continuity different train passed.")


def test_identity_continuity_unknown_missing_vehicle_id():
    print("\nTEST — Identity Continuity Unknown Missing Vehicle ID")

    old_obs = {
        "vehicle_id": None,
        "line_id": "metropolitan",
        "station_id": "940GZZLUALD",
        "destination": "Aldgate",
        "towards": "Aldgate",
        "time_to_station": 500,
    }

    new_obs = {
        "vehicle_id": "430",
        "line_id": "metropolitan",
        "station_id": "940GZZLUALD",
        "destination": "Aldgate",
        "towards": "Aldgate",
        "time_to_station": 300,
    }

    result = evaluate_identity_continuity(old_obs, new_obs)

    print(result)

    assert result["identity_state"] == "unknown"
    assert result["confidence"] == 0

    print("Identity continuity unknown missing vehicle ID passed.")


def test_context_selection_layer(old_snapshot: dict, new_snapshot: dict) -> None:
    print("\nTEST — Context Selection Layer")

    context_result = select_tracking_context(
        old_snapshot,
        new_snapshot
    )

    assert context_result is not None, "Expected shared context to exist"

    selected_context = context_result["selected_context"]

    print("Selected context:")
    print(selected_context)

    assert "station_id" in selected_context
    assert "line_id" in selected_context
    assert "vehicle_id" in selected_context

    print("Policy:", context_result["selected_policy"])

    print("Context selection layer passed.")


def test_tracking_prototype(old_snapshot: dict, new_snapshot: dict) -> None:
    print("\nTEST — Tracking Prototype")

    result = run_tracking_prototype(
        old_snapshot,
        new_snapshot
    )

    assert result is not None, "Expected tracking result"

    print("Tracking result:")
    print(result)

    assert "context_result" in result
    assert "comparison_result" in result

    print("Tracking prototype passed.")

def test_summary_layer():
    print("\nTEST — Summary / Interpretation Layer")

    history = create_comparison_history()

    fake_history_items = [
        {
            "classification_result": {
                "prediction_transition": "closer_to_station"
            }
        },
        {
            "classification_result": {
                "prediction_transition": "closer_to_station"
            }
        },
        {
            "classification_result": {
                "prediction_transition": "unchanged_to_station"
            }
        }
    ]

    for item in fake_history_items:
        history.append(item)

    summary = summarize_comparison_history(history)

    print(summary)

    assert summary["dominant_transition"] == "closer_to_station"
    assert summary["transition_counts"]["closer_to_station"] == 2
    assert summary["transition_counts"]["unchanged_to_station"] == 1
    assert summary["sample_size"] == 3

    print("Summary layer passed.")


def test_real_tracking_history(old_snapshot: dict, new_snapshot: dict) -> None:
    print("\nTEST — Real Tracking History")

    history = create_comparison_history()

    tracking_result = run_tracking_prototype(old_snapshot, new_snapshot)

    update_comparison_history(history, tracking_result)

    summary = summarize_comparison_history(history)

    print("History:", list(history))
    print("Summary:", summary)

    assert len(history) == 1
    assert summary["sample_size"] == 1
    assert summary["dominant_transition"] in [
        "closer_to_station",
        "further_from_station",
        "unchanged_to_station",
        "unknown"
    ]

    print("Real tracking history passed.")

def test_three_snapshot_tracking_history():
    print("\nTEST — Three Snapshot Tracking History")

    history = create_comparison_history()

    snapshot_a = transform_snapshot(grouping_logic(requests_data(station_ids)))
    time.sleep(30)

    snapshot_b = transform_snapshot(grouping_logic(requests_data(station_ids)))
    time.sleep(30)

    snapshot_c = transform_snapshot(grouping_logic(requests_data(station_ids)))

    result_ab = run_tracking_prototype(snapshot_a, snapshot_b)
    update_comparison_history(history, result_ab)

    result_bc = run_tracking_prototype(snapshot_b, snapshot_c)
    update_comparison_history(history, result_bc)

    summary = summarize_comparison_history(history)

    print("History:", list(history))
    print("Summary:", summary)

    assert len(history) == 2
    assert summary["sample_size"] == 2

    print("Three snapshot tracking history passed.")


def test_run_scope_tracking_cycle(old_snapshot: dict, new_snapshot: dict):
    print("\nTEST — Run Scope Tracking Cycle")

    histories = {}

    station_id = "940GZZLUUXB"
    line_id = "metropolitan"

    result = run_scope_tracking_cycle(
        old_snapshot,
        new_snapshot,
        station_id,
        line_id,
        histories
    )

    print("Cycle result:", result)

    assert isinstance(result, dict)

    assert result["station_id"] == station_id
    assert result["line_id"] == line_id

    assert "contexts" in result
    assert "tracking_results" in result
    assert "histories" in result
    assert "tracker_states" in result

    assert isinstance(result["contexts"], list)
    assert isinstance(result["tracking_results"], list)
    assert isinstance(result["histories"], dict)
    assert isinstance(result["tracker_states"], list)

    assert len(result["tracking_results"]) == len(result["contexts"])

    print("Run scope tracking cycle passed.")

def test_tracker_state_to_dict():
    print("\nTEST — Tracker State To Dict")

    tracker_state = {
        "vehicle_id": "323",
        "station_id": "940GZZLUKSX",
        "line_id": "piccadilly",
        "platform_name": "Westbound - Platform 5",
        "station_name": "King's Cross St. Pancras Underground Station",
        "destination": "Heathrow Terminal 5 Underground Station",
        "towards": "Heathrow",
        "movement_state": "closer_to_station",
        "latest_eta_seconds": 45,
        "latest_eta_minutes": 0.75,
        "history_sample_size": 3,
        "transition_counts": {"closer_to_station": 3},
        "summary_status": "completed",
    }

    result = tracker_state_to_dict(tracker_state)

    print(result)

    assert result["vehicle_id"] == "323"
    assert result["station_id"] == "940GZZLUKSX"
    assert result["line_id"] == "piccadilly"
    assert result["time_to_station"] == 45
    assert result["time_to_station_minutes"] == 0.75
    assert result["movement_state"] == "closer_to_station"

    assert "history_sample_size" not in result
    assert "transition_counts" not in result
    assert "summary_status" not in result

    print("Tracker state serialization passed.")


def test_build_tracker_snapshot():
    print("\nTEST — Build Tracker Snapshot")

    tracker_states = [
        {
            "vehicle_id": "323",
            "station_id": "940GZZLUKSX",
            "line_id": "piccadilly",
            "platform_name": "Westbound - Platform 5",
            "station_name": "King's Cross St. Pancras Underground Station",
            "destination": "Heathrow Terminal 5 Underground Station",
            "towards": "Heathrow",
            "movement_state": "closer_to_station",
            "latest_eta_seconds": 45,
            "latest_eta_minutes": 0.75,
        }
    ]

    snapshot = build_tracker_snapshot(
        station_id="940GZZLUKSX",
        line_id="piccadilly",
        tracker_states=tracker_states
    )

    print(snapshot)

    assert snapshot["station_id"] == "940GZZLUKSX"
    assert snapshot["line_id"] == "piccadilly"
    assert "snapshot_timestamp" in snapshot
    assert isinstance(snapshot["tracker_states"], list)
    assert len(snapshot["tracker_states"]) == 1

    state = snapshot["tracker_states"][0]

    assert state["vehicle_id"] == "323"
    assert state["time_to_station"] == 45
    assert "transition_counts" not in state

    print("Tracker snapshot build passed.")

'''
------------Board---------------
'''

def build_current_board_for_scope(snapshot, station_id, line_id):
    board = []

    contexts = get_contexts_for_scope(snapshot, station_id, line_id)

    for context in contexts:
        candidates = collect_vehicle_observations(
            snapshot,
            station_id,
            line_id,
            context["vehicle_id"]
        )

        selected = select_closest_observation_for_station(candidates)

        if selected is None:
            continue

        board.append(selected["selected_observation"])

    return sorted(
        board,
        key=lambda obs: obs["time_to_station"]
    )


def flatten_station_scopes(tracked_stations):
    scopes = []

    for station in tracked_stations:
        for line in station["lines"]:
            scopes.append({
                "station_id": station["station_id"],
                "station_name": station["station_name"],
                "line_id": line["line_id"],
                "line_name": line["line_name"],
            })

    return scopes
'''
------------Main---------------
'''

run_live_multi_scope_tracker(
    TRACKING_SCOPES,
    poll_interval=30
)

"""
old_raw_result = requests_data(station_ids)
old_grouped = grouping_logic(old_raw_result)
old_snapshot = transform_snapshot(old_grouped)

time.sleep(30)

new_raw_result = requests_data(station_ids)
new_grouped = grouping_logic(new_raw_result)
new_snapshot = transform_snapshot(new_grouped)


# Layer tests
test_snapshot_access_layer(old_snapshot)
test_snapshot_access_layer_invalid_cases(old_snapshot)
test_selection_layer(old_snapshot)

# Identity continuity tests
test_identity_continuity_same_train()
test_identity_continuity_different_train()
test_identity_continuity_unknown_missing_vehicle_id()

# Snapshot-to-snapshot tests
test_context_selection_layer(old_snapshot, new_snapshot)
test_tracking_prototype(old_snapshot, new_snapshot)

# History / summary tests
test_summary_layer()
test_real_tracking_history(old_snapshot, new_snapshot)

# Optional next milestone
test_three_snapshot_tracking_history()

# Multi-vehicle scope testing
test_run_scope_tracking_cycle(old_snapshot, new_snapshot)


test_tracker_state_to_dict()
test_build_tracker_snapshot()
"""
