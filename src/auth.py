"""Authenticate to Salesforce by reading the access token from sf cli."""

import json
import logging
import subprocess
from dataclasses import dataclass

from simple_salesforce import Salesforce

logger = logging.getLogger(__name__)


@dataclass
class SfCredentials:
    access_token: str
    instance_url: str


def get_sf_credentials(org_alias: str) -> SfCredentials:
    """Run `sf org display` and extract access token + instance URL.

    Requires prior login via `sf org login web --alias <org_alias>`.
    """
    cmd = ["sf", "org", "display", "--json"]
    if org_alias:
        cmd.extend(["--target-org", org_alias])

    logger.info("Running: %s", " ".join(cmd))

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
    except FileNotFoundError:
        raise RuntimeError(
            "sf cli not found. Install with: npm install -g @salesforce/cli"
        )
    except subprocess.CalledProcessError as e:
        stderr = e.stderr.strip() if e.stderr else ""
        raise RuntimeError(
            f"sf org display failed (exit {e.returncode}). "
            f"Have you logged in? Run: sf org login web --alias {org_alias}\n"
            f"stderr: {stderr}"
        )

    try:
        data = json.loads(result.stdout)
    except json.JSONDecodeError as e:
        raise RuntimeError(
            f"Failed to parse sf org display output: {e}\n"
            f"Raw output: {result.stdout[:200]}"
        )
    sf_result = data.get("result", {})

    access_token = sf_result.get("accessToken")
    instance_url = sf_result.get("instanceUrl")

    if not access_token or not instance_url:
        raise RuntimeError(
            "Could not extract accessToken/instanceUrl from sf org display output. "
            f"Keys found: {list(sf_result.keys())}"
        )

    logger.info("Authenticated to %s", instance_url)
    return SfCredentials(access_token=access_token, instance_url=instance_url)


def connect(org_alias: str) -> Salesforce:
    """Get a simple-salesforce Salesforce connection using sf cli credentials."""
    creds = get_sf_credentials(org_alias)
    return Salesforce(instance_url=creds.instance_url, session_id=creds.access_token)
