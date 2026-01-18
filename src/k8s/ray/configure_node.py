from __future__ import annotations

import json
import logging
import os
import socket
import time

import typer
from k8s.ray.ray_head import initialize_ray_head
from kubernetes import client, config
from kubernetes.client.models.v1_pod import V1Pod
from pydantic import BaseModel, computed_field
from utility.logging import configure_logging


def node_resource_str(node_id: int) -> str:
    """
    Generate a resource string for a node based on its ID.
    This is used to specify resources for Ray actors.
    """
    return f"Node{node_id}"


logger = configure_logging(
    __name__,
    level=logging.INFO,
)

app = typer.Typer(
    help="Configuration scripts",
    pretty_exceptions_show_locals=False,
    pretty_exceptions_enable=False,
)


class WorkerNodeEnv(BaseModel):
    K8S_POD_NAME: str
    RANK: int
    JOBSET_NAME: str

    @classmethod
    def from_env(cls):
        K8S_POD_NAME = os.environ["K8S_POD_NAME"]
        RANK = int(os.environ["RANK"])

        return cls(
            # MASTER_ADDR=os.environ["MASTER_ADDR"],
            # MASTER_PORT=int(os.environ["MASTER_PORT"]),
            JOBSET_NAME=os.environ["JOBSET_NAME"],
            K8S_POD_NAME=K8S_POD_NAME,
            RANK=RANK,
        )

    @computed_field
    @property
    def ray_head_job_name(self) -> str:
        return f"{self.JOBSET_NAME}-rayhead-0"


class RayHeadNodeEnv(BaseModel):
    K8S_POD_NAME: str
    JOBSET_NAME: str

    @classmethod
    def from_env(cls):
        K8S_POD_NAME = os.environ["K8S_POD_NAME"]
        JOBSET_NAME = os.environ["JOBSET_NAME"]

        return cls(K8S_POD_NAME=K8S_POD_NAME, JOBSET_NAME=JOBSET_NAME)

    @computed_field
    @property
    def ray_head_job_name(self) -> str:
        return f"{self.JOBSET_NAME}-rayhead-0"


def get_head_pod_ip(
    label_selector: str,
    namespace: str | None = None,
) -> str:
    """
    Return the Pod IP of the first pod matching `label_selector` in `namespace`.

    Looks up namespace from POD_NAMESPACE env var if not provided.
    Raises RuntimeError if no matching pod is found or no IP is assigned yet.
    """
    ns: str = namespace or os.environ.get("POD_NAMESPACE", "redmod")

    # Use the mounted ServiceAccount inside the pod
    config.load_incluster_config()
    # config.load_config()
    v1 = client.CoreV1Api()

    pods = v1.list_namespaced_pod(namespace=ns, label_selector=label_selector).items
    running_pods: list[V1Pod] = [pod for pod in pods if pod.status.phase == "Running"]

    assert len(running_pods) == 1, f"{len(running_pods)=}, {label_selector=}"
    pod_ip = getattr(running_pods[0].status, "pod_ip", None)
    assert isinstance(pod_ip, str), f"Pod IP not assigned yet for pod {running_pods[0].metadata=}"
    # head_pod = json.loads(running_pods[0])
    # print(type(running_pods[0]))
    return pod_ip


def get_head_ip_with_timeout(
    label_selector: str,
    namespace: str | None = None,
    timeout: float = 600.0,
    interval: float = 2.0,
) -> str:
    """
    Retry get_head_pod_ip for up to `timeout` seconds (default 2 minutes).

    Polls every `interval` seconds until a running pod is found or timeout is reached.
    Raises TimeoutError if no running pod is found within the timeout period.
    """
    import time

    start = time.monotonic()
    last_error = None

    while True:
        elapsed = time.monotonic() - start
        if elapsed > timeout:
            error_msg = f"Could not find running pod with {label_selector=} after {timeout:.1f}s"
            if last_error:
                error_msg += f". Last error: {last_error}"
            raise TimeoutError(error_msg)

        try:
            return get_head_pod_ip(label_selector=label_selector, namespace=namespace)
        except Exception as e:
            last_error = str(e)
            logger.debug(f"Attempt to get head pod IP failed: {e}. Retrying... ({elapsed:.1f}s elapsed)")
            time.sleep(interval)


HEAD_SCRIPT_ENV = {
    "NO_RESET_PROCS": "1",
}


@app.command(name="head")
def run_ray_head(submit_script: str | None = None):
    head_env = RayHeadNodeEnv.from_env()
    logger.info(f"Head env: {head_env}")
    head_pod_ip = get_head_ip_with_timeout(label_selector=f"batch.kubernetes.io/job-name={head_env.ray_head_job_name}")
    with initialize_ray_head(address=head_pod_ip):
        logger.info("Ray head node is running.")

        if submit_script:
            logger.info(f"Executing submit script: {submit_script}")
            import subprocess

            # CHECKPOINT_BASEDIR=  f"/mnt/shared/{head_env.JOBSET_NAME}",
            CHECKPOINT_BASEDIR = "/mnt/shared/test"
            os.makedirs(CHECKPOINT_BASEDIR, exist_ok=True)

            script_env = {
                **os.environ,
                **HEAD_SCRIPT_ENV,
                "MASTER_ADDR": "127.0.0.1",
                "CHECKPOINT_BASEDIR": CHECKPOINT_BASEDIR,
            }

            result = subprocess.run(submit_script, shell=True, capture_output=False, env=script_env)
            logger.info(f"Submit script exited with code {result.returncode}")
        else:
            import time

            while True:
                time.sleep(6000)
                print("Ray head node is still running...")


def is_tcp_open(host: str, port: int, timeout: float = 0.5) -> bool:
    """Return True if a TCP connection to (host, port) succeeds within timeout."""
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def wait_for_service(
    host: str,
    port: int,
    interval: float = 1.0,
    announce_every: int | None = 5,
    timeout: float = 300.0,
) -> None:
    """
    Poll (host, port) once per `interval` seconds until it's reachable.
    Prints a status line every `announce_every` seconds if provided.
    Raises TimeoutError if service is not reachable within `timeout` seconds (default 5 minutes).
    """
    start = time.monotonic()
    tries = 0
    while True:
        elapsed = time.monotonic() - start
        if elapsed > timeout:
            raise TimeoutError(f"Service at {host}:{port} not reachable after {timeout:.1f}s")

        if is_tcp_open(host, port):
            waited = time.monotonic() - start
            print(
                f"✅ Service is reachable at {host}:{port} (waited {waited:.1f}s)",
                flush=True,
            )
            return
        tries += 1
        if announce_every is not None and (tries * interval) % max(announce_every, 1) < 1e-9:
            print(f"⏳ Waiting for {host}:{port} ... ({elapsed:.0f}s elapsed)", flush=True)
        time.sleep(interval)


@app.command(name="worker")
def launch_ray():
    worker_env = WorkerNodeEnv.from_env()
    logger.info(f"Worker env: {worker_env}")

    # Wait until the head node is ready
    head_pod_ip = get_head_ip_with_timeout(label_selector=f"batch.kubernetes.io/job-name={worker_env.ray_head_job_name}")
    logger.info(f"Head pod IP: {head_pod_ip}")
    resources = {node_resource_str(worker_env.RANK): 1.0}

    wait_for_service(head_pod_ip, 6379)

    logger.info("Ray is ready")
    import ray.scripts.scripts as ray_cli

    ray_cli.start.main(
        # URL interpolation is so troll
        args=(
            [
                f"--address={head_pod_ip}:6379",
                "--num-gpus=8",
                f"--resources={json.dumps(resources)}",
                "--block",
            ]
        ),
        prog_name="ray_worker",
        # Otherwise click will os.exit on completion
        standalone_mode=False,
    )


if __name__ == "__main__":
    app()
