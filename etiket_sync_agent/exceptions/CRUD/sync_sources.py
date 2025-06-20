class SyncSourceError(Exception):
    """Base exception for sync source errors."""
    pass

class SyncSourceNameAlreadyExistsError(SyncSourceError):
    """Raised when a sync source name already exists."""
    def __init__(self, name: str):
        self.name = name
        super().__init__(f"Sync source with name '{name}' already exists.")

class SyncSourceInvalidDefaultScopeError(SyncSourceError):
    """Raised when the provided default scope UUID is invalid."""
    def __init__(self, scope_uuid: str):
        self.scope_uuid = scope_uuid
        super().__init__(f"Invalid default scope UUID: '{scope_uuid}'. Scope not found.")

class SyncSourceDefaultScopeRequiredError(SyncSourceError):
    """Raised when a default scope is required for the source type but not provided."""
    def __init__(self):
        super().__init__("A default scope is required for this sync source type but was not provided.")

class SyncSourceConfigDataValidationError(SyncSourceError):
    """Raised when config_data fails validation for the given source type."""
    def __init__(self, message: str, original_exception: Exception | None = None):
        self.message = message
        self.original_exception = original_exception
        super().__init__(f"Invalid config data: {message}")

class SyncSourceNotFoundError(SyncSourceError):
    """Raised when a sync source cannot be found."""
    def __init__(self, source_id: int):
        self.source_id = source_id
        super().__init__(f"Sync source with ID {source_id} not found.")

