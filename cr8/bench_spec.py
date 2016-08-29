
import os
from os.path import basename
import json
import toml


class Instructions:
    def __init__(self,
                 statements=None,
                 statement_files=None,
                 data_files=None):
        self.statements = statements or []
        self.statement_files = statement_files or []
        self.data_files = data_files or []

    @staticmethod
    def from_dict(d):
        return Instructions(
            statements=d.get('statements', []),
            statement_files=d.get('statement_files', []),
            data_files=d.get('data_files', [])
        )


class Spec:
    def __init__(self, setup, teardown, queries=None, load_data=None, meta=None, **metadata):
        self.setup = setup
        self.teardown = teardown
        self.queries = queries
        self.load_data = load_data
        self.meta = metadata
        if meta:
            self.meta.update(meta)

    @staticmethod
    def from_dict(d, **metadata):
        return Spec(
            setup=Instructions.from_dict(d.get('setup', {})),
            teardown=Instructions.from_dict(d.get('teardown', {})),
            meta=d.get('meta', {}),
            queries=d.get('queries', []),
            load_data=d.get('load_data', []),
            **metadata
        )

    @staticmethod
    def from_json_file(filename):
        with open(filename, 'r', encoding='utf-8') as spec_file:
            return Spec.from_dict(json.load(spec_file), name=basename(filename))

    @staticmethod
    def from_toml_file(filename):
        with open(filename, 'r', encoding='utf-8') as spec_file:
            return Spec.from_dict(toml.loads(spec_file.read()), name=basename(filename))

    @staticmethod
    def from_python_file(filename):
        with open(filename, 'r', encoding='utf-8') as f:
            global_vars = {
                'Spec': Spec,
                'Instructions': Instructions
            }
            code = compile(f.read(), filename, 'exec')
            exec(code, global_vars)
            spec = global_vars['spec']
            spec.meta['name'] = spec.meta.get('name', basename(filename))
            return spec


spec_loaders = {
    '.json': Spec.from_json_file,
    '.toml': Spec.from_toml_file,
    '.py': Spec.from_python_file
}


def load_spec(spec_file):
    ext = os.path.splitext(spec_file)[1]
    loader = spec_loaders[ext]
    return loader(spec_file)
