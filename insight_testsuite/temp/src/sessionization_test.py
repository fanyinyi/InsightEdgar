'''
This is unittest part for seesionization.py
'''

import unittest
import datetime
from sessionization import Session, Request

'''
This is the test for Session Class
'''
class SessionTest(unittest.TestCase):
    def test_init(self):
        r = Request("101.81.133.jja,2017-06-30,00:00:00,0.0,1608552.0,0001047469-17-004337,-index.htm,200.0,80251.0,1.0,0.0,0.0,9.0,0.0,")
        session = Session(r)
        self.assertEqual(session.ip, "101.81.133.jja")
        self.assertIsInstance(session.start_time, datetime.datetime)
        self.assertIsInstance(session.end_time, datetime.datetime)
        self.assertEqual(session.start_time.strftime('%Y-%m-%d %H:%M:%S'), "2017-06-30 00:00:00")
        self.assertEqual(session.end_time.strftime('%Y-%m-%d %H:%M:%S'), "2017-06-30 00:00:00")
        self.assertEqual(session.start_time, session.end_time)
        self.assertEqual(session.num_docs, 1)

    def test_update(self):
        r = Request("107.23.85.jfd,2017-06-30,00:00:00,0.0,1136894.0,0000905148-07-003827,-index.htm,200.0,3021.0,1.0,0.0,0.0,10.0,0.0,")
        session = Session(r)
        new_r = Request("107.23.85.jfd,2017-06-30,00:00:01,0.0,1136894.0,0000905148-07-003827,-index.htm,200.0,3021.0,1.0,0.0,0.0,10.0,0.0,")
        session.update(new_r)
        self.assertEqual(session.num_docs, 2)
        self.assertEqual(session.end_time.strftime('%Y-%m-%d %H:%M:%S'), "2017-06-30 00:00:01")

'''
This is the test for Request Class
'''
class RequestTest(unittest.TestCase):
    def test_init(self):
        line = "107.23.85.jfd,2017-06-30,00:00:00,0.0,1136894.0,0000905148-07-003827,-index.htm,200.0,3021.0,1.0,0.0,0.0,10.0,0.0,"
        request = Request(line)
        self.assertEqual(request.ip, "107.23.85.jfd")
        self.assertEqual(request.time.strftime('%Y-%m-%d %H:%M:%S'), "2017-06-30 00:00:00")
        self.assertEqual(request.cik, "1136894.0")
        self.assertEqual(request.accession, "0000905148-07-003827")
        self.assertEqual(request.extension, "-index.htm")


if __name__ == '__main__':
    unittest.main()