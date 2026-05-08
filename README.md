# OpenRelik worker - Chainsaw Hunt

## Description

This worker integrates [Chainsaw](https://github.com/WithSecureLabs/chainsaw) into OpenRelik to hunt through Windows Event Log files (EVTX) using Chainsaw's own detection rules and Sigma rules.

The worker runs `chainsaw hunt` across all EVTX files provided as input, producing one CSV output file per detection category. It uses:
- Chainsaw's built-in EVTX detection rules
- Sigma rules (bundled with the Chainsaw release)
- The `sigma-event-logs-all.yml` field mapping

The Chainsaw binary and all rules are bundled into the Docker image at build time. Each new image build automatically downloads the latest Chainsaw release.

## Deploy

Add the following to your OpenRelik `docker-compose.yml`:

```yaml
openrelik-worker-chainsaw:
    container_name: openrelik-worker-chainsaw
    image: ghcr.io/z3f1r0/openrelik-worker-chainsaw:latest
    restart: always
    environment:
      - REDIS_URL=redis://openrelik-redis:6379
      - OPENRELIK_PYDEBUG=0
    volumes:
      - ./data:/usr/share/openrelik/data
    command: "celery --app=src.app worker --task-events --concurrency=4 --loglevel=INFO -Q openrelik-worker-chainsaw"
    # ports:
      # - 5678:5678 # For debugging purposes.
```

## Test

```
uv sync --group test
uv run pytest -s --cov=.
```
