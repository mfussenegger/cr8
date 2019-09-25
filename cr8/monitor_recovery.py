import argh

from cr8.clients import client
from cr8 import aio

@argh.arg('--hosts', help='CrateDB hosts', type=str)
def monitor_recovery(hosts=None):
    with client(hosts) as c:
        pass
