
import json


class Result:
    def __init__(self,
                 version_info,
                 statement,
                 started,
                 ended,
                 repeats,
                 concurrency,
                 stats,
                 bulk_size=1):
        self.version_info = version_info
        self.statement = statement
        self.bulk_size = bulk_size
        # need ts in ms in crate
        self.started = int(started * 1000)
        self.ended = int(ended * 1000)
        self.repeats = repeats
        self.concurrency = concurrency
        self.__dict__.update(**stats)

    def __str__(self):
        return json.dumps(self.__dict__)
