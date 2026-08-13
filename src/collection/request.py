import requests


TFL_API_BASE_URL = "https://api.tfl.gov.uk"


def request_station_data(station_id: str) -> dict | None:
    try:
        response = requests.get(
            f"{TFL_API_BASE_URL}/StopPoint/{station_id}/Arrivals",
            timeout=10,
        )
        response.raise_for_status()
    except requests.RequestException:
        return None

    return {
        "station_id": station_id,
        "status_code": response.status_code,
        "raw_json": response.json(),
    }


def requests_data(station_ids: list[str]) -> list[dict]:
    return [
        result
        for station_id in station_ids
        if (result := request_station_data(station_id)) is not None
    ]


def request_tube_stop_points() -> list[dict]:
    stop_points = []
    page = 1

    while True:
        response = requests.get(
            f"{TFL_API_BASE_URL}/StopPoint/Mode/tube",
            params={"page": page},
            timeout=20,
        )
        response.raise_for_status()
        payload = response.json()

        page_items = payload.get("stopPoints", [])
        if not isinstance(page_items, list):
            raise ValueError("TfL stopPoints response must be a list.")

        stop_points.extend(page_items)
        total = payload.get("total", len(stop_points))

        if not page_items or len(stop_points) >= total:
            return stop_points

        page += 1
