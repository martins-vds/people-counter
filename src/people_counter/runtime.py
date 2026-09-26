"""Reusable pipeline runtime contracts."""


class RuntimeCompatibilityError(ValueError):
    """Raised when a loaded runtime cannot serve a pipeline configuration."""
