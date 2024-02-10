try:
    from importlib.metadata import PackageNotFoundError, version
except ModuleNotFoundError:  # pragma:nocover
    from importlib_metadata import PackageNotFoundError, version

try:
    __version__ = version("cr8")
except PackageNotFoundError:  # pragma: no cover
    __version__ = "unknown"
