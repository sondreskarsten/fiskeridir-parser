"""BarentsWatch AIS client for parse-time enrichment.

Credentials pulled from Secret Manager at runtime. Token cached in-memory
for one run (~1h validity, one run well under that).
"""

import base64
import time

import requests
from google.cloud import secretmanager


TOKEN_URL = "https://id.barentswatch.no/connect/token"
LIVE_BASE = "https://live.ais.barentswatch.no"


def _read_secret(project, secret_id, version="latest"):
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project}/secrets/{secret_id}/versions/{version}"
    return client.access_secret_version(name=name).payload.data.decode()


class BarentsWatchClient:

    def __init__(self, project, client_id_secret, client_secret_secret, scope="ais"):
        self._client_id = _read_secret(project, client_id_secret)
        self._client_secret = _read_secret(project, client_secret_secret)
        self._scope = scope
        self._token = None
        self._token_expires_at = 0
        self._session = requests.Session()

    def _ensure_token(self):
        if self._token and time.time() < self._token_expires_at - 300:
            return
        r = requests.post(
            TOKEN_URL,
            data={
                "grant_type": "client_credentials",
                "client_id": self._client_id,
                "client_secret": self._client_secret,
                "scope": self._scope,
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=30,
        )
        r.raise_for_status()
        d = r.json()
        self._token = d["access_token"]
        self._token_expires_at = time.time() + int(d["expires_in"])

    def latest_ais(self):
        """Full snapshot from /v1/latest/ais. Used to build callSign -> (lat, lon, ts)."""
        self._ensure_token()
        r = self._session.get(
            f"{LIVE_BASE}/v1/latest/ais",
            headers={"Authorization": f"Bearer {self._token}", "Accept": "application/json"},
            timeout=120,
        )
        r.raise_for_status()
        return r.json()
