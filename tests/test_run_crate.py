from unittest import main, TestCase
from doctest import DocTestSuite
from cr8 import run_crate
from cr8.run_crate import AddrConsumer


lines1 = [
    "[2016-06-11 19:10:16,141][INFO ][node                     ] [Ankhi] initialized",
    "[2016-06-11 19:10:16,141][INFO ][node                     ] [Ankhi] starting ...",
    "[2016-06-11 19:10:16,171][INFO ][http                     ] [Ankhi] publish_address {10.68.2.10:4200}, bound_addresses {[::]:4200}",
    "[2016-06-11 19:10:16,188][INFO ][discovery                ] [Ankhi] crate/GJAvonoFSfS3Y1IaUPTqfA"
]


lines2 = [
    "[2016-06-11 21:26:53,798][INFO ][node                     ] [Rex Mundi] starting ...",
    "[2016-06-11 21:26:53,828][INFO ][http                     ] [Rex Mundi] bound_address {inet[/0:0:0:0:0:0:0:0:4200]}, publish_address {inet[/192.168.0.19:4200]}",
]


lines3 = [
    "[2016-06-15 22:18:36,639][INFO ][node                     ] [crate] starting ...",
    "[2016-06-15 22:18:36,755][INFO ][http                     ] [crate] publish_address {localhost/127.0.0.1:42203}, bound_addresses {127.0.0.1:42203}, {[::1]:42203}, {[fe80::1]:42203}",
    "[2016-06-15 22:18:36,774][INFO ][transport                ] [crate] publish_address {localhost/127.0.0.1:4300}, bound_addresses {127.0.0.1:4300}, {[::1]:4300}, {[fe80::1]:4300}",
    "[2016-06-15 22:18:36,779][INFO ][discovery                ] [crate] Testing42203/Eroq_ZAgT4CDpF_gzh4tcA",
]

lines4 = [
    "[2016-06-16 10:27:20,074][INFO ][node                     ] [Selene] starting ...",
    "[2016-06-16 10:27:20,150][INFO ][http                     ] [Selene] bound_address {inet[/192.168.43.105:4200]}, publish_address {inet[Haudis-MacBook-Pro.local/192.168.43.105:4200]}",
    "[2016-06-16 10:27:20,165][INFO ][transport                ] [Selene] bound_address {inet[/192.168.43.105:4300]}, publish_address {inet[Haudis-MacBook-Pro.local/192.168.43.105:4300]}",
    "[2016-06-16 10:27:20,185][INFO ][discovery                ] [Selene] crate/h9moKMrATmCElYXjfad5Vw",
]

ipv6_psql = "[2017-10-26T15:31:44,654][INFO ][psql                     ] [Deichselspitze] publish_address {[::1]:5432}, bound_addresses {[::1]:5432}"
ipv6_http = "[2017-10-26T15:31:44,666][INFO ][i.c.p.h.CrateNettyHttpServerTransport] [Deichselspitze] publish_address {[::1]:44200}, bound_addresses {[::1]:44200}"

crate12_psql = "[2017-02-01T15:43:37,736][INFO ][psql                     ] [8f64DTi] publish_address {127.0.0.1:5432}, bound_addresses {127.0.0.1:5432}"
crate12_http = "[2017-02-01T15:43:37,793][INFO ][o.e.h.HttpServer         ] [8f64DTi] publish_address {127.0.0.1:4200}, bound_addresses {127.0.0.1:4200}"
crate12_transport = "[2017-02-01T15:43:37,783][INFO ][o.e.t.TransportService   ] [8f64DTi] publish_address {127.0.0.1:4300}, bound_addresses {127.0.0.1:4300}"


def get_match(lines):
    for line in lines:
        protocol, addr = AddrConsumer._parse(line)
        if protocol:
            return protocol, addr


class AddrParseTest(TestCase):

    def test_http_address(self):
        self.assertEqual(get_match(lines1), ('http', '10.68.2.10:4200'))
        self.assertEqual(get_match(lines2), ('http', '192.168.0.19:4200'))
        self.assertEqual(get_match(lines3), ('http', '127.0.0.1:42203'))
        self.assertEqual(get_match(lines4), ('http', '192.168.43.105:4200'))

    def test_crate12_psql(self):
        self.assertEqual(AddrConsumer._parse(crate12_psql), ('psql', '127.0.0.1:5432'))
        self.assertEqual(AddrConsumer._parse(crate12_http), ('http', '127.0.0.1:4200'))
        self.assertEqual(AddrConsumer._parse(crate12_transport), ('transport', '127.0.0.1:4300'))

    def test_ipv6_http(self):
        self.assertEqual(AddrConsumer._parse(ipv6_http), ('http', '[::1]:44200'))

    def test_ipv6_psql(self):
        self.assertEqual(AddrConsumer._parse(ipv6_psql), ('psql', '[::1]:5432'))


def load_tests(loader, tests, ignore):
    tests.addTests(DocTestSuite(run_crate))
    return tests


if __name__ == "__main__":
    main()
