from __future__ import annotations

import json
import logging
import os
from contextlib import contextmanager
from typing import cast

import ray
import ray.scripts.scripts as ray_cli
from pydantic import AnyUrl, UrlConstraints
from utility.logging import configure_logging


logger = configure_logging(__name__, level=logging.DEBUG)


@contextmanager
def temp_env(environ: dict[str, str | None]):
    """
    Context manager that temporarily sets environment variables.
    It restores the original values after the context is exited.
    """
    # Store the original values

    original_environ = {key: os.environ.get(key, None) for key in environ}

    try:
        # Set the new path
        for key, value in environ.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        yield original_environ.copy()
    finally:
        # Restore the original values
        for key, original_value in original_environ.items():
            if original_value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = original_value


# This is really stupid - if we try to start ray with the default LD_LIBRARY_PATH
# that is set by uv managed by devenv, then we get /home/ubuntu/documents/induction-labs/repos/modeling/.devenv/state/venv/lib/python3.12/site-packages/ray/core/src/ray/gcs/gcs_server: /lib/x86_64-linux-gnu/libc.so.6: version `GLIBC_2.36' not found (required by /nix/store/7n3q3rgy5382di7ccrh3r6gk2xp51dh7-gcc-14.2.1.20250322-lib/lib/libstdc++.so.6)
# because it tries to use the glibc from the venv, which is not compatible with the system glibc.
# So we need to set the LD_LIBRARY_PATH to the system glibc and gcc libraries
# I'm not sure why other libraries like numpy and torch work with nix LD_LIBRARY_PATH but ray doesn't,
# I think it is related to ray starting subprocesses that need to link against the system glibc and gcc libraries.


class RayUrl(AnyUrl):
    """ """

    host_required = True

    @property
    def host(self) -> str:
        """The required URL host."""
        return cast(str, self._url.host)  # pyright: ignore[reportAttributeAccessIssue]

    _constraints = UrlConstraints(allowed_schemes=["ray"])


def filter_none[T](l: list[T | None]) -> list[T]:
    return [x for x in l if x is not None]


@contextmanager
def initialize_ray_head(
    address: str | None = None,
    port: int = 6379,
    resources: dict[str, float] | None = None,
):
    logger.debug("Initializing Ray head node...")
    assert not ray.is_initialized(), "Ray is already initialized."

    # resolved_host = services.resolve_ip_for_localhost(HOST)
    # Scheme here is whatever for now idk

    try:
        with temp_env(
            {
                "LD_LIBRARY_PATH": None,
                # TODO(logging): Ray logs are chanelled through `from ray.autoscaler._private.cli_logger import cli_logger`
                # Put these in seperate logs.
                # Also, capture the stdout of the `ray_cli.start` command and put those in seperate logs so we can
                # always enable RAY_LOG_TO_STDERR
                # "RAY_LOG_TO_STDERR": "1",
                "RAY_allow_out_of_band_object_ref_serialization": "0",
                # Set CUDA_VISIBLE_DEVICES to empty string to avoid ray trying to use GPUs on the head node.
                # "CUDA_VISIBLE_DEVICES": "",
            }
        ):
            logger.debug(f"Starting Ray head node at {port=} with {resources=}")
            ray_cli.start.main(
                # URL interpolation is so troll
                args=filter_none(
                    [
                        "--head",
                        f"--node-ip-address={address}" if address else None,
                        f"--port={port!s}",
                        f"--resources={json.dumps(resources)}" if resources else None,
                        "--disable-usage-stats",
                        # TODO:https://github.com/ray-project/ray/issues/45602 runtime_env excludes
                        # TODO: ray dashboard configuration
                        # "--dashboard-host=0.0.0.0",
                    ]
                ),
                prog_name="ray",
                # Otherwise click will os.exit on completion
                standalone_mode=False,
            )
        with temp_env({"RAY_DEDUP_LOGS": "0"}):
            # Wait for the Ray head node to be fully initialized
            ray.init()
        assert ray.is_initialized(), "Ray failed to initialize. Please check the logs for more details."
        logger.debug(f"Ray head node finished initializing at {port}")
        logger.debug(f"{ray.cluster_resources()=}")
        yield
    except Exception as e:
        logger.error(f"Failed to initialize Ray head node: {e}")
        raise e
    finally:
        if ray.is_initialized():
            ray.shutdown()
        else:
            logger.warning("Ray was not initialized, nothing to shutdown.")
        # Ensure the ray processes are cleaned up
        try:
            ray_cli.stop.main(
                # Run with empty args, else will be run with sys.argv
                args=[],
                prog_name="ray",
                standalone_mode=False,
            )
        except Exception as e:
            logger.error(f"Failed to stop Ray head node: {e}")
        logger.info("Ray head node shutdown complete.")
