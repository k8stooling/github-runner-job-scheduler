import hashlib
import logging
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Header, HTTPException, Request
from kubernetes import client, config
from kubernetes.client import ApiException
import yaml

app = FastAPI(title="github-runner-job-scheduler", version="1.0.0")
logger = logging.getLogger("github-runner-job-scheduler")
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))

NAMESPACE = os.getenv("POD_NAMESPACE", "default")
JOB_IMAGE = os.getenv("RUNNER_IMAGE", "actions-runner:latest")
INIT_IMAGE = os.getenv("INIT_CONTAINER_IMAGE", "ghcr.io/actions/runner-token:latest")
RUNNER_TOKEN_DEST = os.getenv("GITHUB_RUNNER_TOKEN_DEST", "/runner-token/runner_token")
GITHUB_URL = os.getenv("GITHUB_URL", "https://github.com")
RUNNER_MAP_PATH = os.getenv("RUNNER_MAP_PATH", "/etc/github-runner-job-scheduler/repo-runner-map.yaml")

_batch_api: client.BatchV1Api | None = None
_repo_map: dict[str, str] = {}
_repo_map_mtime: float | None = None


def get_batch_api() -> client.BatchV1Api:
    global _batch_api
    if _batch_api is None:
        config.load_incluster_config()
        _batch_api = client.BatchV1Api()
    return _batch_api


def get_repo_runner_map() -> dict[str, str]:
    global _repo_map
    global _repo_map_mtime

    config_file = Path(RUNNER_MAP_PATH)
    if not config_file.exists():
        logger.warning("runner map file not found: %s", RUNNER_MAP_PATH)
        return {}

    stat = config_file.stat()
    if _repo_map_mtime is not None and stat.st_mtime == _repo_map_mtime:
        return _repo_map

    with config_file.open("r", encoding="utf-8") as handle:
        parsed = yaml.safe_load(handle) or {}

    map_root = parsed.get("repositories") if isinstance(parsed, dict) else None
    if not isinstance(map_root, dict):
        logger.warning("invalid runner map structure in %s", RUNNER_MAP_PATH)
        _repo_map = {}
        _repo_map_mtime = stat.st_mtime
        return _repo_map

    _repo_map = {
        str(repo): str(label)
        for repo, label in map_root.items()
        if isinstance(repo, str) and isinstance(label, str)
    }
    _repo_map_mtime = stat.st_mtime
    logger.info("loaded %d repository runner mappings", len(_repo_map))
    return _repo_map


def sanitize_job_name(value: str) -> str:
    slug = re.sub(r"[^a-z0-9-]", "-", value.lower())
    slug = re.sub(r"-+", "-", slug).strip("-")
    digest = hashlib.sha1(value.encode("utf-8")).hexdigest()[:8]
    base = (slug[:40] if slug else "repo")
    return f"runner-{base}-{digest}"


def build_job_manifest(repo_full_name: str, runner_label: str) -> client.V1Job:
    job_name = sanitize_job_name(repo_full_name)
    repo_url = f"{GITHUB_URL.rstrip('/')}/{repo_full_name}"

    volumes = [
        client.V1Volume(name="runner-token", empty_dir=client.V1EmptyDirVolumeSource()),
        client.V1Volume(name="runner-work", empty_dir=client.V1EmptyDirVolumeSource()),
    ]

    volume_mounts = [
        client.V1VolumeMount(name="runner-token", mount_path="/runner-token"),
        client.V1VolumeMount(name="runner-work", mount_path="/_work"),
    ]

    init_container = client.V1Container(
        name="init-runner-token",
        image=INIT_IMAGE,
        image_pull_policy="IfNotPresent",
        env=[
            client.V1EnvVar(name="GITHUB_REPOSITORY", value=repo_full_name),
            client.V1EnvVar(name="GITHUB_RUNNER_TOKEN_DEST", value=RUNNER_TOKEN_DEST),
            client.V1EnvVar(name="GITHUB_URL", value=os.getenv("GITHUB_API_URL", "https://api.github.com")),
            client.V1EnvVar(
                name="GITHUB_TOKEN",
                value_from=client.V1EnvVarSource(
                    secret_key_ref=client.V1SecretKeySelector(
                        name=os.getenv("GITHUB_SECRET_NAME", "github-secrets"),
                        key=os.getenv("GITHUB_TOKEN_KEY", "GITHUB_TOKEN"),
                    )
                ),
            ),
        ],
        volume_mounts=[client.V1VolumeMount(name="runner-token", mount_path="/runner-token")],
    )

    run_container = client.V1Container(
        name="runner",
        image=JOB_IMAGE,
        image_pull_policy="IfNotPresent",
        command=["/bin/bash", "-c"],
        args=[
            "/app/config.sh --unattended --ephemeral --url "
            f"{repo_url} --token $(cat {RUNNER_TOKEN_DEST}) --labels {runner_label} "
            "&& /app/run.sh"
        ],
        volume_mounts=volume_mounts,
    )

    pod_spec = client.V1PodSpec(
        restart_policy="Never",
        service_account_name=os.getenv("SERVICE_ACCOUNT_NAME", "default"),
        init_containers=[init_container],
        containers=[run_container],
        volumes=volumes,
    )

    metadata = client.V1ObjectMeta(
        name=job_name,
        labels={
            "app.kubernetes.io/name": "github-runner",
            "app.kubernetes.io/managed-by": "github-runner-job-scheduler",
            "github.repository": repo_full_name,
        },
    )

    return client.V1Job(
        api_version="batch/v1",
        kind="Job",
        metadata=metadata,
        spec=client.V1JobSpec(
            ttl_seconds_after_finished=100,
            backoff_limit=0,
            template=client.V1PodTemplateSpec(
                metadata=client.V1ObjectMeta(labels={"job-name": job_name}),
                spec=pod_spec,
            ),
        ),
    )


@app.get("/healthz")
def healthz() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/webhook")
async def webhook(
    request: Request,
    x_github_event: str | None = Header(default=None),
) -> dict[str, Any]:
    if x_github_event != "workflow_job":
        return {"accepted": False, "reason": "ignored event"}

    payload = await request.json()
    action = payload.get("action")
    if action != "queued":
        return {"accepted": False, "reason": "ignored action"}

    repo_full_name = payload.get("repository", {}).get("full_name")
    if not repo_full_name:
        raise HTTPException(status_code=400, detail="repository.full_name is missing")

    runner_label = get_repo_runner_map().get(repo_full_name)
    if not runner_label:
        return {"accepted": False, "reason": "repository not mapped"}

    job_manifest = build_job_manifest(repo_full_name=repo_full_name, runner_label=runner_label)
    try:
        created = get_batch_api().create_namespaced_job(namespace=NAMESPACE, body=job_manifest)
    except ApiException as exc:
        logger.exception("failed creating runner job")
        raise HTTPException(status_code=500, detail=f"job creation failed: {exc.reason}") from exc

    return {
        "accepted": True,
        "job": created.metadata.name if created and created.metadata else None,
        "repository": repo_full_name,
        "label": runner_label,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
