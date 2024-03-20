try:
    from importlib.metadata import version, PackageNotFoundError
    __version__ = version("cr8")
except ImportError:
    try:
        import pkg_resources
        __version__ = pkg_resources.require('cr8')[0].version
    except ImportError:
        __version__ = "unknown"
except PackageNotFoundError:
    __version__ = "unknown"
