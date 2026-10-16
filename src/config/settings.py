# src/config/settings.py

import yaml

def carregar_config(path: str = '/Workspace/Users/rm372054@fiap.com.br/tesouro-transparente-pipeline/src/config/settings.yaml') -> dict:
    """Carrega um arquivo de configuração YAML."""
    with open(path, 'r') as file:
        return yaml.safe_load(file)