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

class WriteDeltaException(DataHandlerException):
    """Lançada especificamente ao falhar a escrita dos datasets das camadas silver e gold."""

    pass


class EnvironmentValidatorException(DataHandlerException):
    """Exceção base para qualquer falha na validação de ambiente (pre-flight check)."""

    pass


class ValidateVolumePathException(EnvironmentValidatorException):
    """Lançada especificamente ao falhar a validação do caminho do Volume da camada bronze."""

    pass


class ValidateSchemaExistsException(EnvironmentValidatorException):
    """Lançada especificamente ao falhar a validação de existência/acesso a um schema do Unity Catalog."""

    pass