
import tempfile
import shutil
from unittest import TestCase
from cr8.run_crate import CrateNode, get_crate
from cr8.reindex import reindex
from cr8.clients import client
from cr8 import aio


class TestReindex(TestCase):

    def setUp(self):
        self._to_stop = []
        self.data_path = tempfile.mkdtemp()
        self.crate_settings = {
            'path.data': self.data_path,
            'cluster.name': 'cr8-reindex-tests',
            'http.port': '44200-44250'
        }

    def teardown(self):
        for node in self._to_stop:
            node.stop()
        self._to_stop = []
        shutil.rmtree(self.data_path, ignore_errors=True)

    def test_reindex(self):
        crate_v3 = CrateNode(
            crate_dir=get_crate('3.x.x'),
            keep_data=True,
            settings=self.crate_settings
        )
        self._to_stop.append(crate_v3)
        crate_v3.start()
        with client(crate_v3.http_url) as c:
            aio.run(c.execute, "create table t (x int)")
            args = (
                (1,),
                (2,),
                (3,),
            )
            aio.run(c.execute_many, "insert into t (x) values (?)", args)
        crate_v3.stop()
        self._to_stop.remove(crate_v3)

        crate_v4 = CrateNode(
            crate_dir=get_crate('4.0.3'),
            keep_data=True,
            settings=self.crate_settings
        )
        self._to_stop.append(crate_v4)
        crate_v4.start()
        reindex(hosts=crate_v4.http_url)
        with client(crate_v4.http_url) as c:
            result = aio.run(c.execute, "SELECT version FROM information_schema.tables WHERE table_name = 't'")
            version = result['rows'][0][0]
            self.assertEqual(version, {'upgraded': None, 'created': '4.0.3'})

            cnt = aio.run(c.execute, 'SELECT count(*) FROM t')['rows'][0][0]
            self.assertEqual(cnt, 3)
