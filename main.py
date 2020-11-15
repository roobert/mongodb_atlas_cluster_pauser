#!/usr/bin/env python
#
# pause or resume mongodb atlas clusters via GCP scheduler and pub/sub event

import json
import os
from base64 import b64decode, b64encode
from urllib.parse import urljoin

import requests
from requests.auth import HTTPDigestAuth


class EventError(Exception):
    pass


class ActionError(Exception):
    pass


def main(event, context):
    print(f"event: {event}")
    print(f"context: {context}")

    try:
        mongodb_atlas_cluster_pauser(event)
    except Exception as error:
        if os.environ.get("DEBUG") == "true":
            raise
        raise SystemExit(f"{error.__class__.__name__}: {error}")


def mongodb_atlas_cluster_pauser(event):
    if not event.get("data"):
        raise EventError(f"no data key in event: {event}")

    payload = json.loads(b64decode(event["data"]))

    try:
        action = payload["action"]
        project_name = payload["project_name"]
        cluster = payload["cluster"]
    except KeyError as error:
        raise EventError(f"'{error}': missing key from event event: {event}")

    print(f"{project_name}/{cluster}: triggering {action}")
    if action == "unpause":
        response = _pause(project_name, cluster, False)
    elif action == "pause":
        response = _pause(project_name, cluster, True)
    else:
        raise EventError(f"unknown action: '{action}'")

    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError as error:
        if action == "pause":
            if error.response.status_code == 409:
                raise ActionError(
                    f"{project_name}/{cluster}: failing to pause cluster: is cluster already paused?"
                )
            if error.response.status_code == 400:
                raise ActionError(
                    f"{project_name}/{cluster}: failing to pause cluster - has cluster already been paused in last 60 minutes?"
                )

        raise ActionError(error) from error


def _pause(project_name, cluster, pause):
    _, project_id = _project_data(project_name)
    data = {"paused": pause}
    return atlas_request(f"groups/{project_id}/clusters/{cluster}", "patch", data)


def _project_data(project_name):
    projects = [
        result
        for result in _project_list()["results"]
        if result["name"] == project_name
    ]

    if len(projects) == 0:
        raise EventError(f"no result found for project name: {project_name}")

    if len(projects) > 1:
        raise EventError(f"multiple results found for project name: {project_name}")

    return projects[0]["orgId"], projects[0]["id"]


def _project_list():
    return atlas_request("groups").json()


def atlas_request(path, request_type="get", data={}):
    try:
        mcli_public_api_key = os.environ["MCLI_PUBLIC_API_KEY"]
        mcli_private_api_key = os.environ["MCLI_PRIVATE_API_KEY"]
    except KeyError as error:
        raise EnvironmentError(error) from error

    headers = {"Accept": "application/json", "Content-Type": "application/json"}
    auth = HTTPDigestAuth(mcli_public_api_key, mcli_private_api_key)
    url = urljoin("https://cloud.mongodb.com/api/atlas/v1.0/", path, "?pretty=true")

    if request_type == "get":
        response = requests.get(url, headers=headers, auth=auth)
    elif request_type == "patch":
        response = requests.patch(url, headers=headers, auth=auth, json=data)
    else:
        raise NameError(f"unknown request type: {request_type}")

    return response


# run this script locally to test it using these parameters..
if __name__ == "__main__":
    data = b64encode(
        json.dumps(
            {
                "project_name": "dev0-document-service0",
                "cluster": "cluster0",
                "action": "pause",
            }
        ).encode("UTF-8")
    )

    event = {"data": data}
    context = {}

    main(event, context)
