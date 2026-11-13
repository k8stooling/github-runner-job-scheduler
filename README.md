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
    jobDefinitions:
        basic:
            labels:
                - "{{ runner_label }}"
            registrationScope: repository
            resources:
                requests:
                    cpu: "{{ cpu | default('250m') }}"
    repositories:
        k8stooling/repo:
            jobDefinition: basic
            variables:
                runner_label: ubuntu-latest
            overrides:
                serviceAccountName: custom
```

`jobDefinitions` is a dictionary of named job definitions. Each repository selects one
with `jobDefinition: <key>` (required).

- Definition string values are Jinja2 templates (sandboxed, undefined variables are an error and the repository is skipped).
- A value that is exactly one `{{ expr }}` keeps its native type (list, number, boolean).
- `variables`: per-repository values available to the templates. Built-ins: `repository`, `owner`, `repo_name`.
- `overrides`: optional top-level fields that replace the rendered definition's fields for that repository (applied after rendering, not templated).

A rendered job definition is a hook configuration object with these fields:

- `labels`: string or list of labels passed to `config.sh --labels`
- `registrationScope`: `repository` or `organization`
- `organization`: required when `registrationScope=organization`
- `serviceAccountName`: optional per-runner Job service account override
- `automountServiceAccountToken`: optional per-runner Job boolean
- `volumes`, `volumeMounts`, `initContainers`, `env`, `resources`: optional Kubernetes-style fragments merged into the generated Job pod
- `githubUrl`, `githubApiUrl`: optional per-hook overrides for GitHub Enterprise targets

Repository scope registers the runner against `<githubUrl>/<owner>/<repo>`.
Organization scope registers the runner against `<githubUrl>/<organization>`.

Example with a richer Ansible runner pod:

```yaml
config:
    runnerMapPath: /etc/github-runner-job-scheduler/repo-runner-map.yaml
    repositories:
        VFCPS-CaaS/systems-inventory:
            labels:
                - ci-automation
            registrationScope: repository
            serviceAccountName: default
            automountServiceAccountToken: false
            volumes:
                - name: aws-profile
                    configMap:
                        name: aws-profile
                        defaultMode: 420
                - name: runner-home
                    emptyDir: {}
                - name: automation-jobs-artifacts
                    persistentVolumeClaim:
                        claimName: automation-jobs-artifacts
            initContainers:
                - name: copy-aws-profile
                    image: public.ecr.aws/docker/library/busybox:latest
                    command: ["/bin/sh", "-c"]
                    args:
                        - |
                            mkdir -p /home-runner/.aws &&
                            cat /aws/config > /home-runner/.aws/config &&
                            cat /aws/credentials > /home-runner/.aws/credentials &&
                            chown -R 1000:1000 /home-runner/.aws &&
                            chmod 755 /home-runner/.aws &&
                            chmod 644 /home-runner/.aws/*
                    volumeMounts:
                        - name: aws-profile
                            mountPath: /aws
                        - name: runner-home
                            mountPath: /home-runner
            env:
                - name: ANSIBLE_CALLBACK_PLUGINS
                    value: /usr/local/lib/python3.12/site-packages/ara/plugins/callback
                - name: ARA_API_CLIENT
                    value: http
                - name: ARA_API_SERVER
                    value: http://ara-server:8000
            volumeMounts:
                - name: aws-profile
                    mountPath: /aws
                - name: runner-home
                    mountPath: /runner
                - name: automation-jobs-artifacts
                    mountPath: /runner/artifacts
            resources:
                requests:
                    cpu: 250m
                    memory: 500Mi
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
- runner args include `--ephemeral --url <repo-or-org-target>`
- base Job volumes always include `runner-token` and `runner-work`
- additional pod fragments come from the matching repository hook entry

## Build

```bash
docker build -t ghcr.io/k8stooling/github-runner-job-scheduler:latest .
```

## Helm

Chart path: `charts/github-runner-job-scaler`

- Enable Gateway API route: `gateway.enabled=true`
- Create a Gateway from this chart: `gateway.enabled=true` and `gateway.create=true`
- Or attach route to an existing Gateway: `gateway.enabled=true` and set `gateway.name`
- Enable Ingress fallback: `ingress.enabled=true`
