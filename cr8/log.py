import json
from functools import partial


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
    output_fmt = output_fmt or 'full'
    if output_fmt == 'full':
        return to_jsonstr(stats)
    return _format_short(stats)


class Logger:

    def __init__(self, output_fmt='full'):
        self.output_fmt = output_fmt

    def info(self, msg):
        print(msg)

    def result(self, result):
        if self.output_fmt == 'full':
            print(to_jsonstr(result.as_dict()))
        else:
            print(_format_short(result.runtime_stats))
