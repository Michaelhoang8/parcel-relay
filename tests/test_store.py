import concurrent.futures
import json
import sqlite3
import tempfile
import unittest
from store import Relay,Conflict,Missing,digest


def shipment(key='one'):
    return {'key':key,'memo':'Fictional stationery parcel','items':[{'sku':'notebook','quantity':3},{'sku':'pen','quantity':5}]}


class RelayTests(unittest.TestCase):
    def setUp(self):self.temp=tempfile.TemporaryDirectory();self.relay=Relay(self.temp.name)
    def tearDown(self):self.temp.cleanup()
    def test_create_retry_conflict_and_atomic_source_rollback(self):
        first=self.relay.create(shipment());self.assertTrue(self.relay.create(shipment())['replayed'])
        reordered=shipment();reordered['items'].reverse();self.assertEqual(self.relay.create(reordered)['event'],first['event'])
        with self.assertRaises(Conflict):self.relay.create({**shipment(),'memo':'changed'})
        with self.relay.connect(self.relay.sender) as db:db.execute("CREATE TRIGGER fail_outbox BEFORE INSERT ON outbox BEGIN SELECT RAISE(ABORT,'injected'); END")
        with self.assertRaises(sqlite3.IntegrityError):self.relay.create(shipment('two'))
        self.assertEqual(len(self.relay.snapshot()['shipments']),1)

    def test_lost_ack_then_retry_applies_effect_once(self):
        event=self.relay.create(shipment())['event'];ident=event['id']
        self.assertEqual(self.relay.deliver(ident,'after_receive')['status'],'ack_lost')
        state=self.relay.snapshot();self.assertFalse(state['outbox'][0]['acknowledged']);self.assertEqual(len(state['inbox']),1)
        result=self.relay.deliver(ident);self.assertTrue(result['duplicate'])
        state=self.relay.snapshot();self.assertTrue(state['outbox'][0]['acknowledged']);self.assertEqual(state['totals'],[{'sku':'notebook','quantity':3},{'sku':'pen','quantity':5}])
        self.relay.deliver(ident,'after_receive');self.assertTrue(self.relay.snapshot()['outbox'][0]['acknowledged'])
        self.assertTrue(Relay(self.temp.name).snapshot()['audit']['ok'])

    def test_before_receive_does_not_change_receiver(self):
        ident=self.relay.create(shipment())['event']['id'];self.relay.deliver(ident,'before_receive')
        state=self.relay.snapshot();self.assertEqual(state['inbox'],[]);self.assertEqual(state['totals'],[])
        self.assertIsNone(state['attempts'][0]['duplicate']);self.relay.deliver(ident);self.assertEqual(len(self.relay.snapshot()['inbox']),1)

    def test_concurrent_creation_and_duplicate_deliveries(self):
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:created=list(pool.map(lambda _:self.relay.create(shipment()),range(16)))
        self.assertEqual(sum(not r['replayed'] for r in created),1);ident=created[0]['event']['id']
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:results=list(pool.map(lambda _:self.relay.deliver(ident),range(20)))
        self.assertEqual(sum(not r['duplicate'] for r in results),1)
        state=self.relay.snapshot();self.assertEqual(len(state['inbox']),1);self.assertEqual(len(state['attempts']),20);self.assertTrue(state['audit']['ok'])

    def test_receiver_failure_rolls_back_inbox_and_partial_totals(self):
        event=self.relay.create(shipment())['event']
        with self.relay.connect(self.relay.receiver) as db:db.execute("CREATE TRIGGER fail_pen BEFORE INSERT ON totals WHEN NEW.sku='pen' BEGIN SELECT RAISE(ABORT,'injected'); END")
        with self.assertRaises(sqlite3.IntegrityError):self.relay.deliver(event['id'])
        state=self.relay.snapshot();self.assertEqual(state['inbox'],[]);self.assertEqual(state['totals'],[]);self.assertEqual(state['attempts'][0]['status'],'receiver_error')
        with self.relay.connect(self.relay.receiver) as db:db.execute('DROP TRIGGER fail_pen')
        self.relay.deliver(event['id']);self.assertEqual(len(self.relay.snapshot()['inbox']),1)

    def test_event_id_conflicts_and_checksum_corruption(self):
        event=self.relay.create(shipment())['event'];self.relay.consume(event)
        corrupt={**event,'memo':'different'}
        with self.assertRaises(ValueError):self.relay.consume(corrupt)
        corrupt['hash']=digest({k:v for k,v in corrupt.items() if k!='hash'})
        with self.assertRaises(Conflict):self.relay.consume(corrupt)
        with self.relay.connect(self.relay.receiver) as db:db.execute("UPDATE totals SET quantity=99 WHERE sku='pen'")
        self.assertFalse(self.relay.snapshot()['audit']['ok'])

    def test_out_of_order_delivery_is_safe_for_additive_effects(self):
        events=[self.relay.create(shipment(str(i)))['event'] for i in range(8)]
        for event in reversed(events):self.relay.deliver(event['id'])
        state=self.relay.snapshot();self.assertEqual(state['totals'],[{'sku':'notebook','quantity':24},{'sku':'pen','quantity':40}]);self.assertTrue(state['audit']['ok'])

    def test_input_bounds(self):
        for changes in [{'key':'bad/key'},{'memo':'\ud800'},{'items':[]},{'items':[{'sku':'x','quantity':True}]},{'items':[{'sku':'x','quantity':1001}]},{'items':[{'sku':'x','quantity':1}]*2}]:
            with self.assertRaises(ValueError):self.relay.create({**shipment(),**changes})
        with self.assertRaises(Missing):self.relay.deliver('f'*32)
        with self.assertRaises(ValueError):self.relay.deliver('f'*32,'unknown')
