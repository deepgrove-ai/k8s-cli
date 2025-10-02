from __future__ import annotations

import logging
import re
import subprocess
from pathlib import Path
from typing import Annotated

import typer
import yaml
from k8s.logging import configure_logging

import secrets
import string
from pydantic import BaseModel, Field
import functools
import shlex

logger = configure_logging(
    __name__,
    level=logging.INFO,
)


def gen_id(length: int = 8) -> str:
    alphabet = string.ascii_letters + string.digits
    return "".join(secrets.choice(alphabet) for _ in range(length))


# TODO: Add -i interactive option to run k8s interactively
def load_k8s_template(path: Path) -> dict:
    import yaml

    assert path.exists(), f"Template file not found: {path}"

    with open(path) as f:
        job_template = yaml.safe_load(f)
    logger.debug(f"Loaded job template from: {path}")
    return job_template


k8s_app = typer.Typer()
# MODELING_BASE_PATH = Path(__file__).parent.parent.parent.parent
# print(f"{MODELING_BASE_PATH=}")


class K8sEnvConfig(BaseModel):
    bake_target: str = Field()
    job_template: str = Field()
    outputs_dir: str = Field(default="eve-outputs")
    home: str = "."

    @property
    def home_path(self) -> Path:
        return Path.cwd() / self.home

    @property
    def job_template_path(self) -> Path:
        return self.home_path / self.job_template

    @property
    def outputs_dir_path(self) -> Path:
        return self.home_path / self.outputs_dir


K8S_ENV_CONFIG_NAME = "k8sconf.toml"


@functools.lru_cache()
def local_k8s_config():
    local_path = Path.cwd() / K8S_ENV_CONFIG_NAME
    assert local_path.exists(), f"Local k8s config not found: {local_path}"
    import tomli

    with open(local_path, "rb") as f:
        data = tomli.load(f)
    return K8sEnvConfig(**data)


@k8s_app.command()
def bake(
    quiet: Annotated[
        bool, typer.Option("--quiet", "-q", help="Hide depot build output")
    ] = False,
    target: Annotated[
        str | None,
        typer.Option(
            "--target",
            help="Target to build",
        ),
    ] = None,
):
    """
    Build Docker image using depot and extract image reference.
    """
    k8s_config = local_k8s_config()
    target = target if target is not None else k8s_config.bake_target

    try:
        logger.info(f"Starting depot bake process... {target=} {k8s_config.home_path=}")

        # Run depot bake --save command
        process = subprocess.Popen(
            ["depot", "bake", target, "--save"],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            universal_newlines=True,
            cwd=k8s_config.home_path,
        )

        output_lines = []
        assert process.stdout is not None, "Process stdout should not be None"
        for line in process.stdout:
            if not quiet:
                print(line, end="")  # Print in real-time
            output_lines.append(line)

        process.wait()
        if process.returncode != 0:
            raise subprocess.CalledProcessError(
                process.returncode, ["depot", "bake", target, "--save"]
            )

        output = "".join(output_lines)

        # Look for image reference pattern (e.g., registry.depot.dev/v2tbx2d1w1:snsd7rnnn4-remote)
        image_pattern = r"registry\.depot\.dev/[a-zA-Z0-9]+:[a-zA-Z0-9\-]+"
        matches = re.findall(image_pattern, output)

        if matches:
            image_ref = matches[-1]  # Take the last match (most recent)
            assert isinstance(image_ref, str), "Image reference should be a string"
            logger.info(f"Successfully built image: {image_ref}")
            print(f"Built image: {image_ref}")
            return image_ref
        else:
            logger.error("Could not extract image reference from depot output")
            print("Error: Could not extract image reference from depot output")
            raise typer.Exit(1)

    except subprocess.CalledProcessError as e:
        logger.error(f"Depot bake failed with exit code {e.returncode}")
        logger.error(f"Error output: {e.stderr}")
        print(f"Error: Depot bake failed - {e.stderr}")
        raise typer.Exit(1) from e
    except FileNotFoundError as e:
        logger.error("depot command not found. Please ensure depot CLI is installed")
        print("Error: depot command not found. Please ensure depot CLI is installed")
        raise typer.Exit(1) from e


# MDL_TEMPLATE_PATH = MODELING_BASE_PATH / "k8s" / "jobs" / "mdl.yaml"

# EVE_TEMPLATE_PATH = MODELING_BASE_PATH / "k8s" / "eval.yaml"


def load_eve_template(path: Path) -> dict:
    """Load the eve k8s job template."""
    assert path.exists(), f"Template file not found: {path}"

    with open(path) as f:
        job_template = yaml.safe_load(f)
    logger.debug(f"Loaded eve job template from: {path}")
    return job_template


# http://100.110.93.44 -> tailscale head ip
# http://34.136.17.148 -> GCP head ip


# eve clicks run --sample-size 5 --print-cmd | mdl k8s eve ByteDance-Seed/UI-TARS-1.5-7B
# eve clicks run --num-workers 32 --print-cmd | mdl k8s eve ByteDance-Seed/UI-TARS-1.5-7B


@k8s_app.command()
def eve(
    # checkpoint_dirs: Annotated[
    #     list[str], typer.Argument(help="Eve checkpoint directories (gs:// paths)")
    # ],
    eval_cmd: Annotated[
        str | None,
        typer.Option(
            "--eval-cmd",
            help="Evaluation command to run. If not provided, reads from stdin",
        ),
    ] = None,
    image: Annotated[
        str | None,
        typer.Option(
            "--image",
            help="Docker image to use. If not provided, will run bake with provided target",
        ),
    ] = None,
    quiet: Annotated[
        bool, typer.Option("--quiet", "-q", help="Hide output from the command")
    ] = False,
    context: Annotated[
        str | None,
        typer.Option(
            "--context",
            help="Kubernetes context to use. If not provided, uses current context",
        ),
    ] = None,
    dry_run: Annotated[
        bool,
        typer.Option(
            "--dry-run",
            help="Perform a dry run without actually submitting the job",
        ),
    ] = False,
):
    """
    Submit eve jobs to kubernetes with checkpoint directories and evaluation command.
    """
    # Get eval_cmd from stdin if not provided

    k8s_config = local_k8s_config()
    if eval_cmd is None:
        logger.info("Reading evaluation command from stdin...")
        try:
            eval_cmd = input().strip()
            logger.info("Read evaluation command: %s", eval_cmd)
        except (EOFError, KeyboardInterrupt):
            logger.error("No evaluation command provided")
            raise typer.Exit(1) from None

    # Parse the eval_cmd string as a list
    try:
        eval_cmd_list = shlex.split(eval_cmd)

        # import ast
        # eval_cmd_list = ast.literal_eval(eval_cmd)
        if not isinstance(eval_cmd_list, list):
            raise ValueError("Command must be a list")
    except (ValueError, SyntaxError) as e:
        logger.error(f"Failed to parse evaluation command: {eval_cmd}. Error: {e}")
        raise typer.Exit(1) from e

    # Get the image - either from parameter or by running bake with target 'eve'
    if image is None:
        logger.info(
            f"No image provided, running bake with target {k8s_config.bake_target=} ..."
        )
        image = bake(
            quiet=quiet,
        )
    else:
        logger.info(f"Using provided image: {image}")

    logger.info(f"Submitting eval command: {eval_cmd}, image: {image}")

    from kubernetes import client

    from k8s.context import load_kubernetes_config

    load_kubernetes_config(context=context)

    # Create batch API client
    batch_v1 = client.BatchV1Api()

    from datetime import datetime

    # Submit a job for each checkpoint directory
    # for checkpoint_dir in checkpoint_dirs:
    # logger.info(f"Processing checkpoint: {checkpoint_dir}")

    # Load the eve YAML template (fresh copy for each job)
    job_template = load_eve_template(k8s_config.job_template_path)

    # Modify the job template
    container = job_template["spec"]["template"]["spec"]["containers"][0]

    # Update image
    container["image"] = image
    logger.debug(f"Updated container image to: {image}")

    container["args"] = eval_cmd_list
    job_template["spec"]["template"]["metadata"]["annotations"]["container-image"] = (
        image
    )

    logger.debug(f"Updated command args: {container['args']}")
    current_timestamp = datetime.now().strftime("%Y-%m-%dT%H-%M-%S")

    # Save the modified k8s yaml config
    output_dir = k8s_config.outputs_dir_path
    output_dir.mkdir(parents=True, exist_ok=True)
    yaml_config_path = output_dir / f"{current_timestamp}.yaml"

    try:
        with open(yaml_config_path, "w") as f:
            yaml.dump(job_template, f, default_flow_style=False)
        logger.info(f"Saved k8s config to: {yaml_config_path}")
    except Exception as e:
        logger.error(f"Failed to save k8s config: {e}")
        raise typer.Exit(1) from e

    # Get namespace from the job template
    namespace = job_template["metadata"]["namespace"]

    try:
        # Submit the job
        if dry_run:
            response = batch_v1.create_namespaced_job(
                namespace=namespace, body=job_template, dry_run="All"
            )
            logger.info(f"Dry run successful - job would be created for ")
        else:
            response = batch_v1.create_namespaced_job(
                namespace=namespace, body=job_template
            )
            job_name = response.metadata.name  # type: ignore[attr-defined]
            logger.info(f"Successfully submitted eve job: {job_name} for ")

    except Exception as e:
        logger.error(f"Failed to submit eve job  to Kubernetes: {e}")
        raise typer.Exit(1) from e

    logger.info(f"Finished processing")


if __name__ == "__main__":
    k8s_app()
