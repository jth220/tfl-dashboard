import json
import requests
from datetime import datetime
from datetime import timezone
import time
from pathlib import Path

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
