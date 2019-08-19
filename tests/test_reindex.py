
import tempfile
import shutil
from unittest import TestCase
from cr8.run_crate import CrateNode, get_crate
from cr8.reindex import reindex
from crate.client import connect


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
        with connect(crate_v3.http_url) as conn:
            cur = conn.cursor()
            cur.execute("create table t (x int)")
            args = (
                (1,),
                (2,),
                (3,),
            )
            cur.executemany("insert into t (x) values (?)", args)
        crate_v3.stop()
        self._to_stop.remove(crate_v3)

        crate_v4 = CrateNode(
            crate_dir=get_crate('4.0.3'),
            keep_data=True,
            settings=self.crate_settings
        )
        self._to_stop.append(crate_v4)
        crate_v4.start()
        reindex(crate_v4.http_url)
        with connect(crate_v4.http_url) as conn:
            cur = conn.cursor()
            cur.execute("SELECT version FROM information_schema.tables WHERE table_name = 't'")
            version, = cur.fetchone()
            self.assertEqual(version, {'upgraded': None, 'created': '4.0.3'})

            cur.execute('SELECT count(*) FROM t')
            cnt, = cur.fetchone()
            self.assertEqual(cnt, 3)
