import hashlib
import hmac
import copy
import json
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
from jinja2 import StrictUndefined
from jinja2.exceptions import TemplateError
from jinja2.sandbox import SandboxedEnvironment

class HealthzFilter(logging.Filter):
    """Filter to suppress healthz access logs unless in DEBUG mode."""

    def filter(self, record: logging.LogRecord) -> bool:
        if "GET /healthz" in record.getMessage():
            return logging.getLogger("github-runner-job-scheduler").isEnabledFor(
                logging.DEBUG
            )
        return True


app = FastAPI(title="github-runner-job-scheduler", version="1.0.0")
logger = logging.getLogger("github-runner-job-scheduler")
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
# Add filter to suppress healthz logs unless in DEBUG mode
logging.getLogger("uvicorn.access").addFilter(HealthzFilter())

NAMESPACE = os.getenv("POD_NAMESPACE", "default")
JOB_IMAGE = os.getenv("RUNNER_IMAGE", "actions-runner:latest")
INIT_IMAGE = os.getenv("INIT_CONTAINER_IMAGE", "ghcr.io/actions/runner-token:latest")
RUNNER_TOKEN_DEST = os.getenv("GITHUB_RUNNER_TOKEN_DEST", "/runner-token/runner_token")
GITHUB_URL = os.getenv("GITHUB_URL", "https://github.com")
RUNNER_MAP_PATH = os.getenv("RUNNER_MAP_PATH", "/etc/github-runner-job-scheduler/repo-runner-map.yaml")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "")
WEBHOOK_SECRET_REQUIRED = os.getenv("WEBHOOK_SECRET_REQUIRED", "true").lower() == "true"
JOB_BACKOFF_LIMIT = int(os.getenv("JOB_BACKOFF_LIMIT", "10"))
JOB_TTL_SECONDS = int(os.getenv("JOB_TTL_SECONDS", "86400"))
RUNNER_WORK_VOLUME_TYPE = os.getenv("RUNNER_WORK_VOLUME_TYPE", "ephemeral").strip().lower()
RUNNER_WORK_SIZE = os.getenv("RUNNER_WORK_SIZE", "20Gi")
RUNNER_WORK_STORAGE_CLASS = os.getenv("RUNNER_WORK_STORAGE_CLASS", "").strip()
RUNNER_WORK_ACCESS_MODE = os.getenv("RUNNER_WORK_ACCESS_MODE", "ReadWriteOnce")

_batch_api: client.BatchV1Api | None = None
_repo_map: dict[str, dict[str, Any]] = {}
_repo_map_mtime: float | None = None


def runner_work_volume() -> dict[str, Any]:
    if RUNNER_WORK_VOLUME_TYPE != "ephemeral":
        return {"name": "runner-work", "emptyDir": {}}
    spec: dict[str, Any] = {
        "accessModes": [RUNNER_WORK_ACCESS_MODE],
        "resources": {"requests": {"storage": RUNNER_WORK_SIZE}},
    }
    if RUNNER_WORK_STORAGE_CLASS:
        spec["storageClassName"] = RUNNER_WORK_STORAGE_CLASS
    return {"name": "runner-work", "ephemeral": {"volumeClaimTemplate": {"spec": spec}}}


def get_batch_api() -> client.BatchV1Api:
    global _batch_api
    if _batch_api is None:
        config.load_incluster_config()
        _batch_api = client.BatchV1Api()
    return _batch_api


def normalize_labels(value: Any) -> str | None:
    if isinstance(value, str) and value.strip():
        return value.strip()

    if isinstance(value, list):
        labels = [str(item).strip() for item in value if str(item).strip()]
        if labels:
            return ",".join(labels)

    return None


def normalize_repo_config(repo_full_name: str, raw_config: Any) -> dict[str, Any] | None:
    if not isinstance(raw_config, dict):
        logger.warning("invalid hook config for %s: expected mapping", repo_full_name)
        return None

    registration_scope = str(raw_config.get("registrationScope", "")).strip().lower()
    if registration_scope not in {"repository", "organization"}:
        logger.warning(
            "invalid registrationScope for %s: %s",
            repo_full_name,
            raw_config.get("registrationScope"),
        )
        return None

    labels = normalize_labels(raw_config.get("labels"))
    if not labels:
        logger.warning("missing or invalid labels for %s", repo_full_name)
        return None

    organization = raw_config.get("organization")
    if registration_scope == "organization":
        if not isinstance(organization, str) or not organization.strip():
            logger.warning("organization scope requires organization for %s", repo_full_name)
            return None
        organization = organization.strip()
    else:
        organization = None

    normalized: dict[str, Any] = {
        "labels": labels,
        "registrationScope": registration_scope,
    }

    if organization:
        normalized["organization"] = organization

    string_fields = {
        "githubUrl",
        "githubApiUrl",
        "serviceAccountName",
    }
    bool_fields = {"automountServiceAccountToken"}
    list_fields = {"volumes", "volumeMounts", "initContainers", "env"}
    dict_fields = {"resources", "podSecurityContext", "containerSecurityContext"}

    for field in string_fields:
        value = raw_config.get(field)
        if value is None:
            continue
        if not isinstance(value, str) or not value.strip():
            logger.warning("invalid %s for %s", field, repo_full_name)
            return None
        normalized[field] = value.strip()

    secret_ref = raw_config.get("githubTokenSecretRef")
    if secret_ref is not None:
        if (
            not isinstance(secret_ref, dict)
            or not isinstance(secret_ref.get("name"), str)
            or not secret_ref["name"].strip()
            or not isinstance(secret_ref.get("key"), str)
            or not secret_ref["key"].strip()
        ):
            logger.warning(
                "invalid githubTokenSecretRef for %s: expected name and key", repo_full_name
            )
            return None
        normalized["githubTokenSecretRef"] = {
            "name": secret_ref["name"].strip(),
            "key": secret_ref["key"].strip(),
        }

    for field in bool_fields:
        value = raw_config.get(field)
        if value is None:
            continue
        if not isinstance(value, bool):
            logger.warning("invalid %s for %s: expected boolean", field, repo_full_name)
            return None
        normalized[field] = value

    for field in list_fields:
        value = raw_config.get(field)
        if value is None:
            continue
        if not isinstance(value, list):
            logger.warning("invalid %s for %s: expected list", field, repo_full_name)
            return None
        normalized[field] = copy.deepcopy(value)

    for field in dict_fields:
        value = raw_config.get(field)
        if value is None:
            continue
        if not isinstance(value, dict):
            logger.warning("invalid %s for %s: expected mapping", field, repo_full_name)
            return None
        normalized[field] = copy.deepcopy(value)

    return normalized


CONNECTION_FIELDS = ("githubUrl", "githubApiUrl", "githubTokenSecretRef")
_jinja_env = SandboxedEnvironment(undefined=StrictUndefined, autoescape=False)
_SINGLE_EXPR = re.compile(r"^\s*\{\{(?P<expr>(?:(?!\{\{|\}\}).)+)\}\}\s*$", re.DOTALL)


def render_value(value: Any, variables: dict[str, Any]) -> Any:
    """Recursively render Jinja2 strings. A string that is exactly one
    `{{ expr }}` keeps the native type of the expression (list, int, bool...)."""
    if isinstance(value, str):
        match = _SINGLE_EXPR.match(value)
        if match:
            return _jinja_env.compile_expression(
                match.group("expr").strip(), undefined_to_none=False
            )(**variables)
        if "{{" in value or "{%" in value:
            return _jinja_env.from_string(value).render(**variables)
        return value
    if isinstance(value, list):
        return [render_value(item, variables) for item in value]
    if isinstance(value, dict):
        return {key: render_value(item, variables) for key, item in value.items()}
    return value


def resolve_repo_config(
    repo_full_name: str, repo_entry: Any, definitions: dict[str, Any]
) -> dict[str, Any] | None:
    if not isinstance(repo_entry, dict):
        logger.warning("invalid repository entry for %s: expected mapping", repo_full_name)
        return None

    definition_name = repo_entry.get("jobDefinition")
    template = definitions.get(definition_name) if isinstance(definition_name, str) else None
    if not isinstance(template, dict):
        logger.warning(
            "unknown jobDefinition for %s: %r", repo_full_name, definition_name
        )
        return None

    variables = repo_entry.get("variables") or {}
    overrides = repo_entry.get("overrides") or {}
    if not isinstance(variables, dict) or not isinstance(overrides, dict):
        logger.warning("variables and overrides must be mappings for %s", repo_full_name)
        return None

    owner, _, repo_name = repo_full_name.partition("/")
    context = {
        **variables,
        "repository": repo_full_name,
        "owner": owner,
        "repo_name": repo_name,
    }

    try:
        rendered = render_value(template, context)
    except TemplateError as exc:
        logger.warning(
            "failed rendering jobDefinition %s for %s: %s",
            definition_name,
            repo_full_name,
            exc,
        )
        return None

    # repo-level GitHub connection settings win over the definition,
    # overrides replace any top-level field of the rendered definition
    connection = {
        key: copy.deepcopy(repo_entry[key])
        for key in CONNECTION_FIELDS
        if repo_entry.get(key) is not None
    }
    return {**rendered, **connection, **copy.deepcopy(overrides)}


def get_repo_runner_map() -> dict[str, dict[str, Any]]:
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

    repositories = parsed.get("repositories") if isinstance(parsed, dict) else None
    definitions = parsed.get("jobDefinitions") if isinstance(parsed, dict) else None
    if not isinstance(repositories, dict) or not isinstance(definitions, dict):
        logger.warning("invalid runner map structure in %s", RUNNER_MAP_PATH)
        _repo_map = {}
        _repo_map_mtime = stat.st_mtime
        return _repo_map

    normalized_map: dict[str, dict[str, Any]] = {}
    for repo, repo_entry in repositories.items():
        if not isinstance(repo, str):
            logger.warning("invalid repository key in %s: %r", RUNNER_MAP_PATH, repo)
            continue

        raw_config = resolve_repo_config(repo, repo_entry, definitions)
        if raw_config is None:
            continue

        normalized_config = normalize_repo_config(repo_full_name=repo, raw_config=raw_config)
        if normalized_config is None:
            continue

        normalized_map[repo] = normalized_config

    _repo_map = normalized_map
    _repo_map_mtime = stat.st_mtime
    logger.info("loaded %d repository runner mappings", len(_repo_map))
    return _repo_map


def sanitize_job_name(value: str) -> str:
    slug = re.sub(r"[^a-z0-9-]", "-", value.lower())
    slug = re.sub(r"-+", "-", slug).strip("-")
    digest = hashlib.sha1(value.encode("utf-8")).hexdigest()[:8]
    base = (slug[:40] if slug else "repo")
    return f"runner-{base}-{digest}"


def sanitize_label(value: str) -> str:
    # Kubernetes label values must consist of alphanumeric, '-', '_' or '.',
    # and must start and end with an alphanumeric character.
    if not isinstance(value, str):
        return ""
    v = re.sub(r"[^A-Za-z0-9-_.]", "-", value)
    v = re.sub(r"^-+", "", v)
    v = re.sub(r"-+$", "", v)
    if not re.match(r"([A-Za-z0-9].*[A-Za-z0-9])$", v):
        # fallback to a short hash when sanitization produces an invalid value
        v = hashlib.sha1(value.encode("utf-8")).hexdigest()[:8]
    return v


def verify_signature(body: bytes, signature_header: str | None, delivery_id: str | None) -> None:
    if WEBHOOK_SECRET_REQUIRED and not WEBHOOK_SECRET:
        logger.error("webhook secret required but not configured delivery=%s", delivery_id)
        raise HTTPException(status_code=503, detail="webhook secret is required but not configured")

    if not WEBHOOK_SECRET:
        logger.warning("webhook secret not set; signature verification disabled delivery=%s", delivery_id)
        return

    if not signature_header:
        logger.warning("missing signature header delivery=%s", delivery_id)
        raise HTTPException(status_code=401, detail="missing X-Hub-Signature-256 header")

    expected = "sha256=" + hmac.new(
        WEBHOOK_SECRET.encode("utf-8"),
        body,
        hashlib.sha256,
    ).hexdigest()
    if not hmac.compare_digest(expected, signature_header):
        logger.warning("invalid signature delivery=%s", delivery_id)
        raise HTTPException(status_code=401, detail="invalid webhook signature")

    logger.info("signature verified delivery=%s", delivery_id)


def build_job_manifest(
    repo_full_name: str,
    hook_config: dict[str, Any],
    delivery_id: str,
    workflow_job_id: int | None = None,
    workflow_run_id: int | None = None,
    workflow_job_name: str | None = None,
    requested_labels: str | list[str] | None = None,
) -> dict[str, Any]:
    # Use delivery_id to ensure unique job names for each webhook
    job_name = sanitize_job_name(f"{repo_full_name}-{delivery_id[:8]}")
    github_url = str(hook_config.get("githubUrl", GITHUB_URL)).rstrip("/")
    github_api_url = str(
        hook_config.get("githubApiUrl", os.getenv("GITHUB_API_URL", "https://api.github.com"))
    )
    token_secret = hook_config.get("githubTokenSecretRef") or {
        "name": os.getenv("GITHUB_SECRET_NAME", "github-secrets"),
        "key": os.getenv("GITHUB_TOKEN_KEY", "GITHUB_TOKEN"),
    }
    registration_scope = hook_config["registrationScope"]
    runner_labels = hook_config["labels"]

    registration_env = [
        {"name": "GITHUB_REPOSITORY", "value": repo_full_name},
    ]
    registration_url = f"{github_url}/{repo_full_name}"

    if registration_scope == "organization":
        registration_env = [
            {"name": "GITHUB_ORGANIZATION", "value": hook_config["organization"]},
        ]
        registration_url = f"{github_url}/{hook_config['organization']}"

    volumes = [
        {"name": "runner-token", "emptyDir": {}},
        runner_work_volume(),
    ]
    volumes.extend(copy.deepcopy(hook_config.get("volumes", [])))

    volume_mounts = [
        {"name": "runner-token", "mountPath": "/runner-token"},
        {"name": "runner-work", "mountPath": "/_work"},
    ]
    volume_mounts.extend(copy.deepcopy(hook_config.get("volumeMounts", [])))

    init_container = {
        "name": "init-runner-token",
        "image": INIT_IMAGE,
        "imagePullPolicy": "IfNotPresent",
        "env": registration_env
        + [
            {"name": "GITHUB_RUNNER_TOKEN_DEST", "value": RUNNER_TOKEN_DEST},
            {"name": "GITHUB_URL", "value": github_api_url},
            {
                "name": "GITHUB_TOKEN",
                "valueFrom": {
                    "secretKeyRef": {
                        "name": token_secret["name"],
                        "key": token_secret["key"],
                    }
                },
            },
        ],
        "volumeMounts": [{"name": "runner-token", "mountPath": "/runner-token"}],
    }

    init_containers = [init_container]
    init_containers.extend(copy.deepcopy(hook_config.get("initContainers", [])))

    run_container: dict[str, Any] = {
        "name": "runner",
        "image": JOB_IMAGE,
        "imagePullPolicy": "IfNotPresent",
        "command": ["/bin/bash", "-c"],
        "args": [
            "/app/config.sh --unattended --ephemeral --url "
            f"{registration_url} --token $(cat {RUNNER_TOKEN_DEST}) --labels {runner_labels} "
            "&& /app/run.sh"
        ],
        "volumeMounts": volume_mounts,
    }

    extra_env = copy.deepcopy(hook_config.get("env", []))
    if extra_env:
        run_container["env"] = extra_env

    resources = hook_config.get("resources")
    if resources:
        run_container["resources"] = copy.deepcopy(resources)

    container_security_context = hook_config.get("containerSecurityContext")
    if container_security_context:
        run_container["securityContext"] = copy.deepcopy(container_security_context)

    pod_spec: dict[str, Any] = {
        "restartPolicy": "Never",
        "serviceAccountName": hook_config.get(
            "serviceAccountName", os.getenv("SERVICE_ACCOUNT_NAME", "default")
        ),
        "initContainers": init_containers,
        "containers": [run_container],
        "volumes": volumes,
    }

    if "automountServiceAccountToken" in hook_config:
        pod_spec["automountServiceAccountToken"] = hook_config["automountServiceAccountToken"]

    pod_security_context = hook_config.get("podSecurityContext")
    if pod_security_context:
        pod_spec["securityContext"] = copy.deepcopy(pod_security_context)

    metadata = {
        "name": job_name,
        "labels": {
            "app.kubernetes.io/name": "github-runner",
            "app.kubernetes.io/managed-by": "github-runner-job-scheduler",
            "github.repository": sanitize_label(repo_full_name),
        },
        "annotations": {
            "scheduler.github.delivery_id": delivery_id,
            "scheduler.github.workflow_job_id": str(workflow_job_id or ""),
            "scheduler.github.workflow_run_id": str(workflow_run_id or ""),
            "scheduler.github.workflow_job_name": workflow_job_name or "",
            "scheduler.github.requested_labels": (
                ",".join(requested_labels)
                if isinstance(requested_labels, list)
                else str(requested_labels or "")
            ),
            "scheduler.created_at": datetime.now(timezone.utc).isoformat(),
        },
    }

    return {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": metadata,
        "spec": {
            "ttlSecondsAfterFinished": JOB_TTL_SECONDS,
            "backoffLimit": JOB_BACKOFF_LIMIT,
            "template": {
                "metadata": {
                    "labels": {
                        "job-name": job_name,
                        "github.repository": sanitize_label(repo_full_name),
                    },
                    "annotations": {
                        "scheduler.github.delivery_id": delivery_id,
                        "scheduler.github.workflow_job_id": str(workflow_job_id or ""),
                        "scheduler.github.workflow_run_id": str(workflow_run_id or ""),
                        "scheduler.github.workflow_job_name": workflow_job_name or "",
                    },
                },
                "spec": pod_spec,
            },
        },
    }


@app.get("/healthz")
def healthz() -> dict[str, str]:
    if logger.isEnabledFor(logging.DEBUG):
        logger.debug("healthz requested")
    return {"status": "ok"}


@app.post("/webhook")
async def webhook(
    request: Request,
    x_github_event: str | None = Header(default=None),
    x_github_delivery: str | None = Header(default=None),
    x_hub_signature_256: str | None = Header(default=None),
) -> dict[str, Any]:
    raw_body = await request.body()
    logger.info(
        "webhook received delivery=%s event=%s signature_present=%s",
        x_github_delivery,
        x_github_event,
        bool(x_hub_signature_256),
    )
    verify_signature(body=raw_body, signature_header=x_hub_signature_256, delivery_id=x_github_delivery)

    if x_github_event != "workflow_job":
        logger.info("ignored event delivery=%s event=%s", x_github_delivery, x_github_event)
        return {"accepted": False, "reason": "ignored event"}

    try:
        payload = json.loads(raw_body)
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=400, detail="invalid JSON payload") from exc

    action = payload.get("action")
    if action != "queued":
        logger.info("ignored action delivery=%s action=%s", x_github_delivery, action)
        return {"accepted": False, "reason": "ignored action"}

    workflow_job = payload.get("workflow_job") if isinstance(payload.get("workflow_job"), dict) else {}
    workflow_job_id = workflow_job.get("id")
    workflow_run_id = workflow_job.get("run_id")
    workflow_job_name = workflow_job.get("name")
    requested_labels = workflow_job.get("labels")

    repo_full_name = payload.get("repository", {}).get("full_name")
    if not repo_full_name:
        raise HTTPException(status_code=400, detail="repository.full_name is missing")

    logger.info(
        "workflow_job queued delivery=%s repository=%s workflow_job_id=%s run_id=%s name=%s labels=%s",
        x_github_delivery,
        repo_full_name,
        workflow_job_id,
        workflow_run_id,
        workflow_job_name,
        requested_labels,
    )

    hook_config = get_repo_runner_map().get(repo_full_name)
    if not hook_config:
        logger.warning("repository not mapped delivery=%s repository=%s", x_github_delivery, repo_full_name)
        return {"accepted": False, "reason": "repository not mapped"}

    job_manifest = build_job_manifest(
        repo_full_name=repo_full_name,
        hook_config=hook_config,
        delivery_id=x_github_delivery,
        workflow_job_id=workflow_job_id if isinstance(workflow_job_id, int) else None,
        workflow_run_id=workflow_run_id if isinstance(workflow_run_id, int) else None,
        workflow_job_name=workflow_job_name if isinstance(workflow_job_name, str) else None,
        requested_labels=requested_labels if isinstance(requested_labels, (str, list)) else None,
    )
    try:
        created = get_batch_api().create_namespaced_job(namespace=NAMESPACE, body=job_manifest)
    except ApiException as exc:
        logger.exception("failed creating runner job delivery=%s repository=%s", x_github_delivery, repo_full_name)
        raise HTTPException(status_code=500, detail=f"job creation failed: {exc.reason}") from exc

    logger.info(
        "job created delivery=%s repository=%s workflow_job_id=%s run_id=%s job=%s",
        x_github_delivery,
        repo_full_name,
        workflow_job_id,
        workflow_run_id,
        created.metadata.name if created and created.metadata else None,
    )

    return {
        "accepted": True,
        "job": created.metadata.name if created and created.metadata else None,
        "repository": repo_full_name,
        "labels": hook_config["labels"],
        "registrationScope": hook_config["registrationScope"],
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
