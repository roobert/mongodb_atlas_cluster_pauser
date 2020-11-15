#!/usr/bin/env python
#
# pause or resume mongodb atlas clusters via GCP scheduler and pub/sub event

import json
import os
from base64 import b64decode, b64encode

from sh import ErrorReturnCode_1, mongocli


class EventError(Exception):
    pass


class ActionError(Exception):
    pass


def main(event, context):
    print(f"event: {event}")
    print(f"context: {context}")

    try:
        mongodb_cluster_pauser(event)
    except Exception as error:
        if os.environ.get("DEBUG") == "true":
            raise
        print(f"{error.__class__.__name__}: {error}")
        exit(1)


def mongodb_cluster_pauser(event):
    if not event.get("data"):
        raise EventError(f"no data key in event: {event}")

    payload = json.loads(b64decode(event["data"]))

    try:
        action = payload["action"]
        project_name = payload["project_name"]
        cluster = payload["cluster"]
    except KeyError as error:
        raise EventError(f"'{error}': missing key from event event: {event}")

    if action == "start":
        _start(project_name, cluster)
    elif action == "pause":
        _pause(project_name, cluster)
    else:
        raise EventError(f"unknown action: '{action}'")


def _environment(project_name):
    org_id, project_id = _project_data(project_name)

    try:
        mcli_public_api_key = os.environ["MCLI_PUBLIC_API_KEY"]
        mcli_private_api_key = os.environ["MCLI_PRIVATE_API_KEY"]
    except KeyError as error:
        raise EnvironmentError(error) from error

    environment = {
        "MCLI_PUBLIC_API_KEY": mcli_public_api_key,
        "MCLI_PRIVATE_API_KEY": mcli_private_api_key,
        "MCLI_ORG_ID": org_id,
        "MCLI_PROJECT_ID": project_id,
        "MCLI_SERVICE": "cloud",
        **os.environ.copy(),
    }

    mongocli_bin_path = os.path.join(os.path.realpath(__file__), "bin")
    environment["PATH"] = f"{mongocli_bin_path}:{environment['PATH']}"

    return environment


# pause calls will fail if the cluster is already paused
def _pause(project_name, cluster):
    try:
        print(
            mongocli.atlas.cluster.pause(
                cluster, "-o=json", _env=_environment(project_name)
            ).stdout.decode("UTF-8")
        )
    except ErrorReturnCode_1 as error:
        raise ActionError(error.stderr.decode("UTF-8").rstrip()) from error


# start calls are idempotent and always(?) succeed..
def _start(project_name, cluster):
    try:
        print(
            mongocli.atlas.cluster.start(
                cluster, "-o=json", _env=_environment(project_name)
            ).stdout.decode("UTF-8")
        )
    except ErrorReturnCode_1 as error:
        raise ActionError(error.stderr.decode("UTF-8").rstrip()) from error


def _project_list():
    return json.loads(mongocli.iam.projects.list("-o=json").stdout.decode("UTF-8"))


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


# run this script locally to test it using these parameters..
if __name__ == "__main__":
    json_data = json.dumps(
        {
            "project_name": "dev0-rating-service0",
            "cluster": "cluster0",
            "action": "start",
        }
    )

    encoded_data = b64encode(json_data.encode("UTF-8"))

    event = {"data": encoded_data}
    context = {}

    main(event, context)
