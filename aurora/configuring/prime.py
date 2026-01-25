import inspect
import os
from pathlib import Path

import dotenv
from envyaml import EnvYAML
from loguru import logger

from aurora.etc.pattern import singleton


@singleton
class IConfig:

    attributes = tuple(["llm"])

    def __init__(self):
        logger.info(
            f"IConfig | path to `config.yaml` = [{str(Path(os.getcwd()) / 'config.yaml')}]"
        )
        config = dict(EnvYAML(Path(os.getcwd()) / "config.yaml", strict=False))
        logger.info("Loaded configuration from `config.yaml`.")
        for k, v in config.items():
            if k.startswith("_"):
                continue
            setattr(self, k, v)
        dotenv.load_dotenv()


@singleton
class AgentsConfig:

    attributes = ("default", "universe")

    def __init__(self):
        logger.info(
            f"AgentsConfig | path to `agents.yaml` = [{str(Path(os.getcwd()) / 'agents.yaml')}]"
        )
        config = dict(EnvYAML(Path(os.getcwd()) / "agents.yaml", strict=False))
        logger.info("Loaded configuration from `agents.yaml`")
        for k, v in config.items():
            if k.startswith("_"):
                continue
            setattr(self, k, v)


Config = IConfig()
AgentsConfigPipeline = AgentsConfig()

__all__ = ["Config", "AgentsConfigPipeline"]
