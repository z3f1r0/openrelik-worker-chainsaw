import os
import shutil
import subprocess
import time
from uuid import uuid4

from celery import signals
from celery.utils.log import get_task_logger

from openrelik_worker_common.file_utils import create_output_file
from openrelik_common.logging import Logger
from openrelik_worker_common.task_utils import create_task_result, get_input_files

from .app import celery

TASK_NAME = "openrelik-worker-chainsaw.tasks.hunt"

CHAINSAW_BINARY = "/chainsaw/chainsaw"
CHAINSAW_RULES = "/chainsaw/rules"
CHAINSAW_SIGMA = "/chainsaw/sigma"
CHAINSAW_MAPPING = "/chainsaw/mappings/sigma-event-logs-all.yml"

TASK_METADATA = {
    "display_name": "Chainsaw Hunt",
    "description": "Hunt through Windows event logs using Chainsaw detection rules and Sigma rules, producing CSV output.",
    "task_config": [],
}

COMPATIBLE_INPUTS = {
    "data_types": [],
    "mime_types": ["application/x-ms-evtx"],
    "filenames": ["*.evtx"],
}

log_root = Logger()
logger = log_root.get_logger(__name__, get_task_logger(__name__))


@signals.task_prerun.connect
def on_task_prerun(sender, task_id, task, args, kwargs, **_):
    log_root.bind(
        task_id=task_id,
        task_name=task.name,
        worker_name=TASK_METADATA.get("display_name"),
    )


def run_chainsaw(command):
    """Run chainsaw and return (stdout, stderr, returncode, command_string).

    Uses subprocess.run so that stdout/stderr buffers are fully drained —
    polling with Popen+PIPE without reading causes a deadlock when chainsaw
    writes enough sigma-rule warnings to fill the OS pipe buffer (~64 KB).
    """
    cmd_str = " ".join(command)
    logger.info(f"Running: {cmd_str}")

    process = subprocess.run(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    return process.stdout, process.stderr, process.returncode, cmd_str


@celery.task(bind=True, name=TASK_NAME, metadata=TASK_METADATA)
def hunt(
    self,
    pipe_result: str = None,
    input_files: list = None,
    output_path: str = None,
    workflow_id: str = None,
    task_config: dict = None,
) -> str:
    """Run Chainsaw hunt on input EVTX files.

    Args:
        pipe_result: Base64-encoded result from the previous Celery task, if any.
        input_files: List of input file dictionaries (unused if pipe_result exists).
        output_path: Path to the output directory.
        workflow_id: ID of the workflow.
        task_config: User configuration for the task.

    Returns:
        Base64-encoded dictionary containing task results.
    """
    log_root.bind(workflow_id=workflow_id)
    logger.info(f"Starting {TASK_NAME} for workflow {workflow_id}")

    self.send_event("task-progress", {"status": "Starting Chainsaw Hunt"})

    output_files = []
    input_files = get_input_files(pipe_result, input_files or [], filter=COMPATIBLE_INPUTS)

    if not input_files:
        logger.warning("No compatible EVTX input files found")
        self.send_event("task-progress", {"status": "No compatible EVTX input files found"})
        return create_task_result(
            output_files=output_files,
            workflow_id=workflow_id,
            command="",
        )

    logger.info(f"Found {len(input_files)} input file(s)")
    self.send_event("task-progress", {"status": f"Found {len(input_files)} EVTX file(s), preparing input"})

    input_temp_dir = os.path.join(output_path, f"input_{uuid4().hex}")
    chainsaw_output_dir = os.path.join(output_path, f"chainsaw_{uuid4().hex}")
    os.makedirs(input_temp_dir)
    os.makedirs(chainsaw_output_dir)

    try:
        for file in input_files:
            src = file.get("path")
            dst = os.path.join(input_temp_dir, os.path.basename(src))
            try:
                os.link(src, dst)
            except OSError:
                shutil.copy2(src, dst)

        command = [
            CHAINSAW_BINARY,
            "hunt",
            input_temp_dir,
            "--rule", CHAINSAW_RULES,
            "--sigma", CHAINSAW_SIGMA,
            "--mapping", CHAINSAW_MAPPING,
            "--csv",
            "--output", chainsaw_output_dir,
            "--full",
            "--skip-errors",
        ]

        self.send_event("task-progress", {"status": "Running Chainsaw hunt"})

        stdout, stderr, returncode, cmd_str = run_chainsaw(command)

        if stdout:
            logger.info(stdout)
        if stderr:
            logger.warning(stderr)

        if returncode != 0:
            logger.error(f"Chainsaw exited with code {returncode}")
            self.send_event("task-progress", {
                "status": "ERROR: Chainsaw exited with non-zero code",
                "returncode": returncode,
                "stderr": stderr,
            })
            raise RuntimeError(f"Chainsaw failed (exit {returncode}): {stderr}")

        self.send_event("task-progress", {"status": "Collecting CSV output files"})

        for filename in sorted(os.listdir(chainsaw_output_dir)):
            if not filename.lower().endswith(".csv"):
                continue
            src_csv = os.path.join(chainsaw_output_dir, filename)
            stem = os.path.splitext(filename)[0]
            output_file = create_output_file(
                output_path,
                display_name=f"{stem}.csv",
                data_type="openrelik:chainsaw:hunt:csv",
            )
            shutil.copy2(src_csv, output_file.path)
            output_files.append(output_file.to_dict())
            logger.info(f"Collected output: {filename}")

        if not output_files:
            logger.info("Chainsaw produced no CSV output (no detections)")
            self.send_event("task-progress", {"status": "No detections found"})
        else:
            self.send_event("task-progress", {
                "status": f"Collected {len(output_files)} CSV file(s)",
            })

    finally:
        shutil.rmtree(input_temp_dir, ignore_errors=True)
        shutil.rmtree(chainsaw_output_dir, ignore_errors=True)

    logger.info(f"Finished {TASK_NAME} for workflow {workflow_id}")
    self.send_event("task-progress", {"status": "Task completed"})

    return create_task_result(
        output_files=output_files,
        workflow_id=workflow_id,
        command=cmd_str,
        meta={},
    )
