"""CLI helpers: capture secret values in memory, never log them."""

import base64
import json
import subprocess


def cli(profile, *args):
    result = subprocess.run(["databricks", *args, "--profile", profile, "-o", "json"],
                            capture_output=True, text=True)
    if result.returncode:
        raise RuntimeError(f"Databricks command failed: {' '.join(args[:2])}")
    return json.loads(result.stdout) if result.stdout.strip() else None


def secret(profile, scope, key):
    return base64.b64decode(cli(profile, "secrets", "get-secret", scope, key)["value"]).decode()


def put_secret(profile, scope, key, value):
    result = subprocess.run(["databricks", "secrets", "put-secret", scope, key, "--profile", profile],
                            input=value, capture_output=True, text=True)
    if result.returncode:
        raise RuntimeError(f"Unable to store secret key {key}")
