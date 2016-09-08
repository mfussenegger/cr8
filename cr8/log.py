
import sys
import functools


stdout = functools.partial(print, file=sys.stdout)
stderr = functools.partial(print, file=sys.stderr)
