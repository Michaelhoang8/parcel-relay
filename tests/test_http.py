import json
import tempfile
import threading
import unittest
from http.client import HTTPConnection
from http.server import ThreadingHTTPServer
from pathlib import Path
from run import Handler
from store import Relay
from test_store import shipment


class HTTPTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory()
        self.server=ThreadingHTTPServer(('127.0.0.1',0),Handler)
        self.server.store=Relay(self.temp.name)
        self.thread=threading.Thread(target=self.server.serve_forever,daemon=True)
        self.thread.start()

    def tearDown(self):
        self.server.shutdown();self.server.server_close();self.thread.join();self.temp.cleanup()

    def request(self,method,path,body=None,headers=None):
        client=HTTPConnection(*self.server.server_address,timeout=3)
        client.request(method,path,None if body is None else json.dumps(body),headers or {'Content-Type':'application/json'})
        response=client.getresponse();raw=response.read();status=response.status
        self.assertIn("frame-ancestors 'none'",response.headers['Content-Security-Policy'])
        mime=response.headers['Content-Type'];client.close()
        return status,json.loads(raw) if raw and mime.startswith('application/json') else raw

    def test_delivery_lifecycle(self):
        status,created=self.request('POST','/api/shipments',shipment())
        self.assertEqual(status,201)
        self.assertEqual(self.request('POST','/api/shipments',shipment())[0],200)
        self.assertEqual(self.request('POST','/api/shipments',{**shipment(),'memo':'Changed'})[0],409)
        path='/api/events/'+created['event']['id']+'/deliver'
        self.assertEqual(self.request('POST',path,{'fault':'after_receive'})[1]['status'],'ack_lost')
        state=self.request('GET','/api/state')[1]
        self.assertFalse(state['outbox'][0]['acknowledged']);self.assertEqual(len(state['inbox']),1)
        self.assertTrue(self.request('POST',path,{'fault':'none'})[1]['duplicate'])
        state=self.request('GET','/api/state')[1]
        self.assertTrue(state['outbox'][0]['acknowledged']);self.assertTrue(state['audit']['ok'])
        self.assertEqual(self.request('POST',path,{'fault':'invalid'})[0],400)
        self.assertEqual(self.request('POST','/api/events/'+'f'*32+'/deliver',{'fault':'none'})[0],404)

    def test_boundaries(self):
        for path in ['/store.py','/data/sender.sqlite3','/.git/config','/../run.py']:
            self.assertEqual(self.request('GET',path)[0],404)
        self.assertEqual(self.request('HEAD','/')[1],b'')
        self.assertEqual(self.request('GET','/',headers={'Host':'external.invalid'})[0],400)
        self.assertEqual(self.request('POST','/api/shipments',{}, {'Content-Type':'application/json','Origin':'https://other.invalid'})[0],403)
        self.assertEqual(self.request('POST','/api/shipments',{}, {'Content-Type':'text/plain'})[0],415)
        self.assertEqual(self.request('POST','/api/shipments',{'x':'a'*65536})[0],413)
        self.assertEqual(self.request('PUT','/api/shipments',{})[0],405)
