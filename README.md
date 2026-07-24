# github-runner-job-scheduler

FastAPI webhook scaler for GitHub Actions `workflow_job` queue events. It creates one ephemeral Kubernetes Job runner per queued workflow job.

## Webhook behavior

- Listens on `POST /webhook`.
- Accepts only `workflow_job` events with `action: queued`.
- Verifies `X-Hub-Signature-256` using a shared webhook secret.
- Uses a repository to runner-label YAML map loaded from a ConfigMap-mounted file.
- Creates one Kubernetes Job via in-cluster ServiceAccount authentication.

## Mapping configuration

The map is provided by Helm values and rendered into a ConfigMap file mounted at
`/etc/github-runner-job-scheduler/repo-runner-map.yaml`.

Example values snippet:

```yaml
config:
    runnerMapPath: /etc/github-runner-job-scheduler/repo-runner-map.yaml
    repositories:
        k8stooling/repo: ubuntu-latest
```

## Webhook secret support

For internet-facing endpoints, configure a GitHub webhook secret and provide it via Kubernetes Secret.

Use an existing secret:

```yaml
webhook:
    requireSignature: true
    secret:
        create: false
        name: github-webhook-secret
        key: WEBHOOK_SECRET
```

Or let Helm create it:

```yaml
webhook:
    requireSignature: true
    secret:
        create: true
        key: WEBHOOK_SECRET
        value: "replace-with-strong-random-secret"
```

## Runner Job details

- `ttlSecondsAfterFinished: 100`
- `initContainer`: defaults to `ghcr.io/actions/runner-token:latest` (override with `INIT_CONTAINER_IMAGE`)
- `runner container`: image from `RUNNER_IMAGE`
- runner args include `--ephemeral --url <repo>`

## Build

```bash
docker build -t ghcr.io/k8stooling/github-runner-job-scheduler:latest .
```

## Helm

Chart path: `charts/github-runner-job-scheduler`

- Enable Gateway API route: `gateway.enabled=true`
- Create a Gateway from this chart: `gateway.enabled=true` and `gateway.create=true`
- Or attach route to an existing Gateway: `gateway.enabled=true` and set `gateway.name`
- Enable Ingress fallback: `ingress.enabled=true`
