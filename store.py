"""Two SQLite stores demonstrating outbox delivery and idempotent consumption."""
import hashlib
import json
import re
import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path


class Conflict(ValueError): pass
class Missing(ValueError): pass


class Connection(sqlite3.Connection):
    def __exit__(self,*args):
        try:return super().__exit__(*args)
        finally:self.close()


def now():return datetime.now(timezone.utc).isoformat(timespec='microseconds')
def canonical(value):return json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=True,allow_nan=False)
def digest(value):return hashlib.sha256(canonical(value).encode()).hexdigest()


def items(raw):
    if not isinstance(raw,list) or not 1<=len(raw)<=8:raise ValueError('Provide 1–8 unique SKU quantities.')
    seen=set();result=[]
    for item in raw:
        if not isinstance(item,dict) or set(item)!={'sku','quantity'} or not isinstance(item['sku'],str) or not re.fullmatch('[A-Za-z0-9][A-Za-z0-9_-]{0,31}',item['sku']) or item['sku'] in seen:raise ValueError('SKUs must be distinct ASCII names of 1–32 characters.')
        if type(item['quantity']) is not int or not 1<=item['quantity']<=1000:raise ValueError('Quantity must be an integer from 1 to 1,000.')
        seen.add(item['sku']);result.append(dict(item))
    return sorted(result,key=lambda x:x['sku'])


class Relay:
    def __init__(self,directory):
        self.directory=Path(directory);self.directory.mkdir(parents=True,exist_ok=True)
        self.sender=self.directory/'sender.sqlite3';self.receiver=self.directory/'receiver.sqlite3'
        with self.connect(self.sender) as db:
            db.execute('PRAGMA journal_mode=WAL')
            db.executescript('''
            CREATE TABLE IF NOT EXISTS shipments(id INTEGER PRIMARY KEY, request_key TEXT NOT NULL UNIQUE, request TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS outbox(id TEXT PRIMARY KEY, shipment_id INTEGER NOT NULL UNIQUE REFERENCES shipments(id), payload TEXT NOT NULL, hash TEXT NOT NULL, acknowledged INTEGER NOT NULL DEFAULT 0 CHECK(acknowledged IN(0,1)));
            CREATE TABLE IF NOT EXISTS attempts(id INTEGER PRIMARY KEY, event_id TEXT NOT NULL REFERENCES outbox(id), fault TEXT NOT NULL, status TEXT NOT NULL, started TEXT NOT NULL, finished TEXT, duplicate INTEGER);
            ''')
        with self.connect(self.receiver) as db:
            db.execute('PRAGMA journal_mode=WAL')
            db.executescript('''
            CREATE TABLE IF NOT EXISTS inbox(id TEXT PRIMARY KEY, payload TEXT NOT NULL, hash TEXT NOT NULL, received TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS totals(sku TEXT PRIMARY KEY, quantity INTEGER NOT NULL CHECK(quantity>0));
            ''')

    def connect(self,path):
        db=sqlite3.connect(path,timeout=10,factory=Connection);db.execute('PRAGMA foreign_keys=ON');return db

    def create(self,body):
        if not isinstance(body,dict) or set(body)!={'key','memo','items'}:raise ValueError('Provide key, memo, and items.')
        if not isinstance(body['key'],str) or not re.fullmatch('[A-Za-z0-9][A-Za-z0-9_-]{0,63}',body['key']):raise ValueError('Request key must be a 1–64 character ASCII name.')
        memo=body['memo']
        if not isinstance(memo,str) or not 1<=len(memo)<=120 or any(ord(c)<32 or 127<=ord(c)<=159 or 0xD800<=ord(c)<=0xDFFF for c in memo):raise ValueError('Memo must contain 1–120 printable characters.')
        normalized={'key':body['key'],'memo':memo,'items':items(body['items'])};request=canonical(normalized)
        with self.connect(self.sender) as db:
            db.execute('BEGIN IMMEDIATE');prior=db.execute('SELECT id,request FROM shipments WHERE request_key=?',(body['key'],)).fetchone()
            if prior:
                if prior[1]!=request:raise Conflict('Request key has different shipment content.')
                event=db.execute('SELECT payload,hash FROM outbox WHERE shipment_id=?',(prior[0],)).fetchone()
                return {'event':{**json.loads(event[0]),'hash':event[1]},'replayed':True}
            if db.execute('SELECT COUNT(*) FROM shipments').fetchone()[0]>=200:raise ValueError('This demo is limited to 200 shipments.')
            known={item['sku'] for row in db.execute('SELECT request FROM shipments') for item in json.loads(row[0])['items']}
            if len(known|{item['sku'] for item in normalized['items']})>64:raise ValueError('This demo is limited to 64 distinct SKUs.')
            ident=db.execute('INSERT INTO shipments(request_key,request) VALUES(?,?)',(body['key'],request)).lastrowid
            event={'id':uuid.uuid4().hex,'version':1,'shipmentId':ident,'memo':memo,'items':normalized['items'],'createdAt':now()};hashed=digest(event)
            db.execute('INSERT INTO outbox(id,shipment_id,payload,hash) VALUES(?,?,?,?)',(event['id'],ident,canonical(event),hashed))
            return {'event':{**event,'hash':hashed},'replayed':False}

    def consume(self,event):
        fields={'id','version','shipmentId','memo','items','createdAt','hash'}
        if not isinstance(event,dict) or set(event)!=fields or type(event['version']) is not int or event['version']!=1 or not isinstance(event['id'],str) or not re.fullmatch('[0-9a-f]{32}',event['id']):raise ValueError('Unsupported event envelope.')
        if type(event['shipmentId']) is not int or not 1<=event['shipmentId']<=200:raise ValueError('Invalid shipment identifier.')
        normalized=items(event['items'])
        payload={k:v for k,v in event.items() if k!='hash'}
        if event['hash']!=digest(payload):raise ValueError('Event checksum mismatch.')
        with self.connect(self.receiver) as db:
            db.execute('BEGIN IMMEDIATE');prior=db.execute('SELECT payload,hash FROM inbox WHERE id=?',(event['id'],)).fetchone()
            if prior:
                if prior!=(canonical(payload),event['hash']):raise Conflict('Event ID already has different content in the receiver.')
                return {'duplicate':True,'eventId':event['id']}
            if db.execute('SELECT COUNT(*) FROM inbox').fetchone()[0]>=200:raise ValueError('Receiver inbox limit reached.')
            known={r[0] for r in db.execute('SELECT sku FROM totals')}
            if len(known|{i['sku'] for i in normalized})>64:raise ValueError('Receiver SKU limit reached.')
            db.execute('INSERT INTO inbox VALUES(?,?,?,?)',(event['id'],canonical(payload),event['hash'],now()))
            for item in normalized:db.execute('INSERT INTO totals VALUES(?,?) ON CONFLICT(sku) DO UPDATE SET quantity=quantity+excluded.quantity',(item['sku'],item['quantity']))
            return {'duplicate':False,'eventId':event['id']}

    def deliver(self,event_id,fault='none'):
        if fault not in ('none','before_receive','after_receive'):raise ValueError('Unknown fault mode.')
        with self.connect(self.sender) as db:
            db.execute('BEGIN IMMEDIATE');row=db.execute('SELECT payload,hash FROM outbox WHERE id=?',(event_id,)).fetchone()
            if not row:raise Missing('Outbox event not found.')
            if db.execute('SELECT COUNT(*) FROM attempts').fetchone()[0]>=2000:raise ValueError('Delivery-attempt limit reached.')
            attempt=db.execute('INSERT INTO attempts(event_id,fault,status,started) VALUES(?,?,?,?)',(event_id,fault,'started',now())).lastrowid
        duplicate=None
        if fault=='before_receive':status='not_received'
        else:
            try:duplicate=self.consume({**json.loads(row[0]),'hash':row[1]})['duplicate']
            except (ValueError,sqlite3.Error):
                with self.connect(self.sender) as db:db.execute('UPDATE attempts SET status=?,finished=? WHERE id=?',('receiver_error',now(),attempt))
                raise
            status='ack_lost' if fault=='after_receive' else 'acknowledged'
        with self.connect(self.sender) as db:
            db.execute('BEGIN IMMEDIATE')
            if status=='acknowledged':db.execute('UPDATE outbox SET acknowledged=1 WHERE id=?',(event_id,))
            db.execute('UPDATE attempts SET status=?,finished=?,duplicate=? WHERE id=?',(status,now(),None if duplicate is None else int(duplicate),attempt))
        return {'attempt':attempt,'eventId':event_id,'status':status,'duplicate':duplicate}

    def snapshot(self):
        with self.connect(self.sender) as db:
            db.execute('BEGIN');shipments=[{'id':r[0],**json.loads(r[1])} for r in db.execute('SELECT id,request FROM shipments ORDER BY id')]
            outbox=[{**json.loads(r[0]),'hash':r[1],'acknowledged':bool(r[2])} for r in db.execute('SELECT payload,hash,acknowledged FROM outbox ORDER BY shipment_id')]
            attempts=[dict(zip(['id','eventId','fault','status','started','finished','duplicate'],r)) for r in db.execute('SELECT id,event_id,fault,status,started,finished,duplicate FROM attempts ORDER BY id')]
        with self.connect(self.receiver) as db:
            db.execute('BEGIN');inbox=[{**json.loads(r[0]),'hash':r[1],'receivedAt':r[2]} for r in db.execute('SELECT payload,hash,received FROM inbox ORDER BY received,id')]
            totals=[{'sku':r[0],'quantity':r[1]} for r in db.execute('SELECT sku,quantity FROM totals ORDER BY sku')]
        expected={};errors=[]
        for entry in inbox:
            if digest({k:v for k,v in entry.items() if k not in ('hash','receivedAt')})!=entry['hash']:errors.append(f'Inbox hash mismatch: {entry["id"]}')
            for item in entry['items']:expected[item['sku']]=expected.get(item['sku'],0)+item['quantity']
        if expected!={r['sku']:r['quantity'] for r in totals}:errors.append('Receiver totals differ from inbox replay.')
        for entry in outbox:
            if digest({k:v for k,v in entry.items() if k not in ('hash','acknowledged')})!=entry['hash']:errors.append(f'Outbox hash mismatch: {entry["id"]}')
        return {'format':'parcel-relay-evidence-v1','shipments':shipments,'outbox':outbox,'inbox':inbox,'attempts':attempts,'totals':totals,'audit':{'ok':not errors,'errors':errors}}
