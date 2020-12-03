import sys, os
import time
import datetime
import dateutil.parser
from copy import deepcopy as copy
from elasticsearch import Elasticsearch as ES
from elasticsearch.helpers import parallel_bulk as bulk

_in_index  = 'manual-links'#sys.argv[1];
_id_index  = 'gesis-test'#sys.argv[2];
_out_index = 'gold-links'#sys.argv[3];
_mal_index = 'bad-links'#sys.argv[4];

_header = ['from_ID','to_ID','verified','annotation_time','annotator'];

_scr_body = { 'query': {'match_all': {} } }; # TODO: If I change from deleting in manual links to a flag then select here everything unchecked
#_scr_body = {'query':{'bool':{'must':[{'term':{'checked': False}}]}}}; #TODO: TEST THIS FIRST!

_ind_body = { '_op_type': 'index',
              '_index':   None,
              '_id':      None,
              '_source': { field:None for field in _header },
              '_type': 'link' #TODO: This is due to outdated ES Version on GWS
        }

_upd_body = { '_op_type': 'update', #TODO: Fix this!
              '_index':   None,
              '_id':      None,
              '_source': { 'doc': { field:None for field in _header } },
              '_type': 'link' #TODO: This is due to outdated ES Version on GWS
        }

_del_body = { '_op_type': 'delete',
              '_index': _in_index,
              '_id': None,
              '_type': 'link' #TODO: This is due to outdated ES Version on GWS
        }

_id_body = { 'query': { 'ids' : { 'type': None, 'values': [None] } } }

_lnk_body = {'query':{'bool':{'must':[{'term':{'from_ID': None}}, {'term':{'to_ID':None}}]}}};

def valid_date(string):
    try:
        then = dateutil.parser.parse(string);
        if datetime.datetime.now(tz=datetime.timezone.utc) > then:
            return then.isoformat();
    except:
        pass;
    return False;

def valid_id(identifier,typ,client):
    body = copy(_id_body);
    body['query']['ids']['type']      = typ;
    body['query']['ids']['values'][0] = identifier;
    results = client.search(index=_id_index,body=body);
    if results['hits']['total'] == 0:
        return False
    return True;

def exists(from_ID,to_ID,client,index):
    body = copy(_lnk_body);
    body['query']['bool']['must'][0]['term']['from_ID'] = from_ID;
    body['query']['bool']['must'][1]['term']['to_ID']   = to_ID;
    results = client.search(index=index,body=body);
    if results['hits']['total'] == 0:
        return False;
    return results['hits']['hits'][0]['_id'];

def update(doc): #TODO: TEST THIS FIRST!
    body = copy(_upd_body);
    body['_index']    = 'manual-links';
    body['_id']       = doc['_id'];
    source            = doc['source'];
    source['checked'] = True;
    body['_source']   = source;
    print(body);
    return body;

def delete(doc):
    body = copy(_del_body);
    body['_id'] = doc['_id'];
    print(body);
    return body;

def check(doc):
    client = ES(['search.gesis.org/es-config/'],scheme='http',port=80,timeout=60);
    source = doc['_source'];
    status = '';
    if (not 'annotation_time' in source) or (not valid_date(source['annotation_time'])):    # annotation time must be valid date in the past
        status += '/ no or invalid annotation_time (remember timezone), should be ISO 8601 or similar, future dates will be rejected /';
    else:
        source['annotation_time'] = valid_date(source['annotation_time']);
    if (not 'annotator' in source) or (not isinstance(source['annotator'],str)):    # annotator must be string
        status += '/ no annotator or not string /';
    if (not 'correct' in source) or (not (isinstance(source['correct'],bool) or source['correct']==None)):    # correct must be bool or None
        status += '/ correct needs to be boolean or none /';
    if (not 'from_ID' in source) or (not valid_id(source['from_ID'],'publication',client)):    # from_ID must be an id in gws
        status += '/ no or nonexistant from_ID /';
    if (not 'to_ID' in source) or (not valid_id(source['to_ID'],'research_data',client)):    # from_ID must be an id in gws
        status += '/ no or nonexistant to_ID /';
    if source['from_ID'] == source['to_ID']:    # to_ID must be an id in gws and different from from_ID
        status += '/ from_ID is the same as to_ID /';
    if status != '':
        source['reason'] = status;
    index            = 'gold-links' if status==''  else 'bad-links';
    existing_id      = exists(source['from_ID'],source['to_ID'],client,index);
    body             = copy(_upd_body) if existing_id else copy(_ind_body) ;
    body['_id']      = existing_id     if existing_id else doc['_id'];
    body['_source']  = {'doc':source}  if existing_id else source;
    body['_index']   = index;
    print(body);
    return body;

def get_links():
    client   = ES(['search.gesis.org/es-config/'],scheme='http',port=80,timeout=60);
    page     = client.search(index=_in_index,scroll='2m',size=100,body=_scr_body);
    sid      = page['_scroll_id'];
    size     = float(page['hits']['total']);
    returned = size;
    page_num = 0;
    while returned > 0:
        for doc in page['hits']['hits']:
            yield check(doc);
            yield delete(doc); #TODO: Maybe replace by another function update(doc); that adds a flag field checked=True
            #yield update(doc);
        try:
            page = client.scroll(scroll_id=sid, scroll='2m');
        except:
            print('WARNING: Some problem occured while scrolling. Sleeping for 3s and retrying...');
            time.sleep(3); continue;
        page_num += 1;
        returned  = len(page['hits']['hits']);


_client = ES(['search.gesis.org/es-config/'],scheme='http',port=80,timeout=60);

while True:

    i = 0;
    for success, info in bulk(_client,get_links()):
        i += 1;
        if not success:
            print('A document failed:', info['index']['_id'], info['index']['error']);
        elif i % 10000 == 0:
            print(i);

    time.sleep(1);
    print(time.time(),end='\r');
