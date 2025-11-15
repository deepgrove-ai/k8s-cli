from __future__ import annotations

from pathlib import Path

from pydantic import Field
from utility.config_class import ConfigClass


class K8sEnvConfig(ConfigClass):
    bake_target: str
    helm_template: str
    outputs_dir: str
    home: str = Field(default=".")

    @property
    def home_path(self) -> Path:
        return Path.cwd() / self.home

    @property
    def helm_template_path(self) -> Path:
        return self.home_path / self.helm_template

    # @property
    # def outputs_dir_path(self) -> Path:
    #     return self.home_path / self.outputs_dir
