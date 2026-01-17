from __future__ import annotations

import json
import logging
import re
import shlex
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Annotated

import tomli
import typer
import yaml
from pydantic import BaseModel
from utility.loading_context import Loading

from k8s.config import K8sEnvConfig
from k8s.logging import configure_logging
from k8s.ray.configure_node import app as ray_app


logger = configure_logging(
    __name__,
    level=logging.INFO,
)


k8s_app = typer.Typer()

K8S_ENV_CONFIG_NAME = "k8sconf.toml"


def local_k8s_config(local_path: Path | None = None):
    if local_path is None:
        local_path = Path.cwd() / K8S_ENV_CONFIG_NAME
    assert local_path.exists(), f"Local k8s config not found: {local_path}"

    with open(local_path, "rb") as f:
        data = tomli.load(f)
    return K8sEnvConfig.deserialize(data)


@k8s_app.command()
def bake(
    quiet: Annotated[bool, typer.Option("--quiet", "-q", help="Hide depot build output")] = False,
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
    output_lines: list[str] = []

    try:
        with Loading(f"Baking depot {target=} {k8s_config.home_path=} ..."):
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

            assert process.stdout is not None, "Process stdout should not be None"
            for line in process.stdout:
                if not quiet:
                    print(line, end="")  # Print in real-time
                output_lines.append(line)

            process.wait()
            if process.returncode != 0:
                raise subprocess.CalledProcessError(process.returncode, ["depot", "bake", target, "--save"])

            output = "".join(output_lines)

            # Look for image reference pattern (e.g., registry.depot.dev/v2tbx2d1w1:snsd7rnnn4-remote)
            image_pattern = r"registry\.depot\.dev/[a-zA-Z0-9]+:[a-zA-Z0-9\-]+"
            matches = re.findall(image_pattern, output)

        if matches:
            image_ref = matches[-1]  # Take the last match (most recent)
            assert isinstance(image_ref, str), "Image reference should be a string"
            logger.info(f"Successfully built image: {image_ref}")
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
        if quiet:
            print("\n".join(output_lines))
        raise typer.Exit(1) from e


def subprocess_run(cmd: list[str], **kwargs):
    try:
        return subprocess.run(
            cmd,
            check=True,
            text=True,
            stderr=subprocess.PIPE,  # 👈 critical
            **kwargs,
        )
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"❌ Command failed {e.returncode=} {e.cmd=}{e.stdout=} {e.stderr}") from e


@k8s_app.command()
def submit(
    helm_template: Annotated[
        str | None,
        typer.Option(
            "--helm-template",
            help="Helm template to use",
        ),
    ] = None,
    outputs_dir: Annotated[
        str | None,
        typer.Option(
            "--outputs-dir",
            help="Outputs directory to use",
        ),
    ] = None,
    values_json: Annotated[
        str,
        typer.Option(
            "--values",
            help="Values to pass to the helm template",
        ),
    ] = "",
    dry_run: Annotated[
        bool,
        typer.Option(
            "--dry-run",
            help="Perform a dry run without actually submitting the job",
        ),
    ] = False,
):
    k8s_config = local_k8s_config()
    helm_template_path = Path(helm_template) if helm_template is not None else k8s_config.helm_template_path
    logger.info(f"Submitting helm template: {helm_template_path} with values: {values_json}")
    set_arg = ["--set-json", f"k8s={values_json}"] if values_json else []
    rendered = subprocess_run(
        [
            "helm",
            "template",
            str(helm_template_path),
            *set_arg,
        ],
        stdout=subprocess.PIPE,
    ).stdout

    # Determine output directory
    output_dir = Path(outputs_dir) if outputs_dir is not None else k8s_config.home_path / k8s_config.outputs_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    # Generate timestamp and save rendered YAML
    current_timestamp = datetime.now().strftime("%Y-%m-%dT%H-%M-%S")
    yaml_output_path = output_dir / f"{current_timestamp}.yaml"

    with open(yaml_output_path, "w") as f:
        f.write(rendered)
    logger.info(f"Saved rendered YAML to: {yaml_output_path}")

    return subprocess_run(
        [
            "kubectl",
            "create",
            *(("--dry-run=server",) if dry_run else []),
            "-f",
            str(yaml_output_path),
        ],
    )


class BakeSubmitArgs(BaseModel):
    values: list[dict]
    image: str | None = None
    dry_run: bool = False
    helm_template: str | None = None
    target: str | None = None
    outputs_dir: str | None = None

    def bake_and_submit(self):
        assert len(self.values) > 0, "At least one value must be provided"
        if self.image is None:
            self.image = bake(target=self.target, quiet=True)

        for value in self.values:
            values_json = json.dumps({**value, "image": self.image})
            submit(
                helm_template=self.helm_template,
                values_json=values_json,
                dry_run=self.dry_run,
                outputs_dir=self.outputs_dir,
            )
        logger.info(f"Submitted {len(self.values)} jobs")


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
    quiet: Annotated[bool, typer.Option("--quiet", "-q", help="Hide output from the command")] = False,
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
        logger.info(f"No image provided, running bake with target {k8s_config.bake_target=} ...")
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
    job_template["spec"]["template"]["metadata"]["annotations"]["container-image"] = image
    logger.debug(f"Updated container image to: {image}")

    container["args"] = eval_cmd_list

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
            response = batch_v1.create_namespaced_job(namespace=namespace, body=job_template, dry_run="All")
            logger.info("Dry run successful - job would be created for ")
        else:
            response = batch_v1.create_namespaced_job(namespace=namespace, body=job_template)
            job_name = response.metadata.name  # type: ignore[attr-defined]
            logger.info(f"Successfully submitted eve job: {job_name} for ")

    except Exception as e:
        logger.error(f"Failed to submit eve job  to Kubernetes: {e}")
        raise typer.Exit(1) from e

    logger.info("Finished processing")


k8s_app.add_typer(ray_app, name="ray")

if __name__ == "__main__":
    k8s_app()
