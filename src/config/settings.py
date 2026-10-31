# src/config/settings.py

import os
import yaml
import logging.config


def carregar_config(
    path: str = os.path.join(os.path.dirname(__file__), "settings.yaml"),
) -> dict:
    """Carrega um arquivo de configuração YAML."""
    with open(path, "r") as file:
        return yaml.safe_load(file)


def configurar_logging(config_logging: dict):
    """Aplica a configuração de logging lida do YAML."""
    logging.config.dictConfig(config_logging)
    logging.getLogger(__name__).info("Logging configurado com sucesso via YAML.")
