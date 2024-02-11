import subprocess
import sys
import time
import unittest

from tests.integration_util import node, setup, teardown, translate


def setUpModule():
    node.start()
    assert node.http_host, "http_url must be available"


def tearDownModule():
    node.stop()


class IntegrationTest(unittest.TestCase):
    """
    Integration tests defined as Python code, derived from README doctest code.

    Rationale: Currently, running the README doctests on
               Windows trips, and hasn't been resolved yet.
    """
    def setUp(self) -> None:
        """
        Provision tables.
        """
        setup()

    def tearDown(self) -> None:
        """
        Destroy tables.
        """
        teardown()

    def cmd(self, command: str):
        """
        Invoke a shell command.
        """
        return subprocess.check_call(translate(command), shell=True)

    def test_connectivity(self):
        command = "cr8 timeit --hosts localhost:4200"
        self.cmd(command)

    @unittest.skip(reason="Windows quoting issue")
    def test_sys_cluster(self):
        command = """echo "SELECT * FROM sys.cluster;" | sed -e 's/\(^"\|"$\)//g' | cr8 timeit --hosts localhost:4200"""
        self.cmd(command)

    @unittest.skip(reason="Windows quoting issue")
    def test_sys_summits(self):
        command = """echo "SELECT * FROM sys.summits ORDER BY height DESC LIMIT 3;" | sed -e 's/\(^"\|"$\)//g' | cr8 timeit --hosts localhost:4200"""
        self.cmd(command)

    def test_insert_fake_data(self):
        command = "cr8 insert-fake-data --hosts localhost:4200 --table x.demo --num-records 200"
        self.cmd(command)

    def test_insert_json(self):
        command = "cat tests/demo.json | cr8 insert-json --table x.demo --hosts localhost:4200"
        self.cmd(command)

    @unittest.skip(reason="Windows quoting issue")
    def test_insert_json_print(self):
        command = """echo '{"name": "Arthur"}' | sed -e "s/\(^'\|'$\)//g" | cr8 insert-json --table mytable"""
        self.cmd(command)

    def test_insert_from_sql(self):
        command = "cr8 insert-fake-data --hosts localhost:4200 --table x.demo --num-records 200"
        self.cmd(command)

        # Synchronize writes.
        # command = """echo "REFRESH TABLE x.demo;" | sed -e 's/\(^"\|"$\)//g' | cr8 timeit --hosts localhost:4200"""
        # self.cmd(command)
        time.sleep(1)

        command = """
        cr8 insert-from-sql \
          --src-uri "postgresql://crate@localhost:5432/doc" \
          --query "SELECT name FROM x.demo" \
          --hosts localhost:4200 \
          --table y.demo
        """
        self.cmd(command)

    def test_run_spec_toml(self):
        command = "cr8 run-spec specs/sample.toml localhost:4200 -r localhost:4200"
        self.cmd(command)

    def test_run_spec_python(self):
        command = "cr8 run-spec specs/sample.py localhost:4200"
        self.cmd(command)
