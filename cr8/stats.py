
import json


class Result:
    def __init__(self,
                 version_info,
                 statement,
                 started,
                 ended,
                 repeats,
                 stats):
        self.version_info = version_info
        self.statement = statement
        # need ts in ms in crate
        self.started = int(started * 1000)
        self.ended = int(ended * 1000)
        self.repeats = repeats
        self.__dict__.update(**stats)

    def __str__(self):
        return json.dumps(self.__dict__)
