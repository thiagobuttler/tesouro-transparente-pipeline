# src/io_utils/exceptions.py

class DataHandlerException(Exception):
    """Exceção base para qualquer falha na camada de I/O."""
    pass

class LoadTesouroTransparenteException(DataHandlerException):
    """Lançada especificamente ao falhar o carregamento do dataset dos dados do tesouro transparente."""
    pass

class LoadSilverGoldException(DataHandlerException):
    """Lançada especificamente ao falhar o carregamento dos datasets das camadas silver e gold."""
    pass
