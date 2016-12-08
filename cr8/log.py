import json
import sys
import contextlib
from functools import partial


_wopen = partial(open, mode='w', encoding='utf-8')
to_jsonstr = partial(json.dumps, sort_keys=True, indent=4)


def _format_short(stats):
    output = ('Runtime (in ms):\n'
              '    mean:    {mean:.3f} ± {error_margin:.3f}')
    values = dict(
        mean=stats['mean'],
        error_margin=stats.get('error_margin', 0.0),
    )
    if stats['n'] > 1:
        output += (
            '\n'
            '    min/max: {min:.3f} → {max:.3f}\n'
            'Percentile:\n'
            '    50:   {p50:.3f} ± {stdev:.3f} (stdev)\n'
            '    95:   {p95:.3f}\n'
            '    99.9: {p999:.3f}'
        )
        percentiles = stats['percentile']
        values.update(dict(
            max=stats['max'],
            min=stats['min'],
            stdev=stats['stdev'],
            p50=percentiles['50'],
            p95=percentiles['95'],
            p999=percentiles['99_9']
        ))
    return output.format(**values)


def format_stats(stats, output_fmt=None):
    output_fmt = output_fmt or 'text'
    if output_fmt == 'json':
        return to_jsonstr(stats)
    return _format_short(stats)


class Logger(contextlib.ExitStack):

    def __init__(self,
                 output_fmt='text',
                 logfile_info=None,
                 logfile_result=None):
        super().__init__()
        info_output = self._open(logfile_info)
        result_output = self._open(logfile_result)
        self.info = partial(print, file=info_output)
        presult = partial(print, file=result_output)
        if output_fmt == 'json':
            self.result = lambda r: presult(to_jsonstr(r.as_dict()))
        else:
            self.result = lambda r: presult(_format_short(r.runtime_stats))

    def _open(self, filepath):
        if filepath:
            return self.enter_context(_wopen(filepath))
        return sys.stdout
