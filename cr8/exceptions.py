# vi: set fileencoding=utf-8
# -*- coding: utf-8; -*-


class NoHttpAddressAvailable(Exception):
    """
    Exception is raised when no HTTP address could be obtained from logfile
    on node start within given time or log size.
    """
    pass

