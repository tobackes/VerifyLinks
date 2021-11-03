#-IMPORTS-----------------------------------------------------------------------------------------------------------------------------------------
import sys, os
import time
import datetime
import dateutil.parser
from copy import deepcopy as copy
from elasticsearch import Elasticsearch as ES
from elasticsearch.helpers import parallel_bulk as bulk
#-------------------------------------------------------------------------------------------------------------------------------------------------
#-GLOBAL OBJECTS----------------------------------------------------------------------------------------------------------------------------------
_in_index  = 'manual-links' #sys.argv[1];
_id_index  = 'gesis-test'   #sys.argv[2];
_out_index = 'gold-links'   #sys.argv[3];
_mal_index = 'bad-links'    #sys.argv[4];

_DELETE = True;

_GWS = True if sys.argv[1].lower() == 'gws' else False;

_SLEEP   = 30                            if _GWS else 3;
_SLEEP_  = 60                            if _GWS else 30;
_ADDRESS = 'search.gesis.org/es-config/' if _GWS else 'svko-skg.gesis.intra/';
_SCHEME  = 'http'                        if _GWS else 'http';
_PORT    = 80                            if _GWS else 9200;
_TIMEOUT = 60                            if _GWS else 60;
_TRIES   = 3;

_header = ['from_ID','to_ID','verified','annotation_time','annotator','to_SubID','from_SubID'];

_scr_body = { 'query': {'match_all': {} } } if _DELETE else {'query':{'bool':{'must_not':[{'term':{'checked': True}}]}}};

_ind_body = { '_op_type': 'index',
              '_index':   None,
              '_id':      None,
              '_source': { field:None for field in _header },
        }                                                       if not _GWS else { '_op_type': 'index',
                                                                                   '_index':    None,
                                                                                   '_id':       None,
                                                                                   '_source':   { field:None for field in _header },
                                                                                   '_type':     'link'
                                                                                  }

_upd_body = { '_op_type': 'update',
              '_index':   None,
              '_id':      None,
              '_source': { 'doc': { field:None for field in _header } },
        }                                                                   if not _GWS else { '_op_type': 'update',
                                                                                               '_index':    None,
                                                                                               '_id':       None,
                                                                                               '_source':  { 'doc': { field:None for field in _header } },
                                                                                               '_type':    'link'
                                                                                             }

_del_body = { '_op_type': 'delete',
              '_index': _in_index,
              '_id': None,
            }                           if not _GWS else {  '_op_type': 'delete',
                                                            '_index': _in_index,
                                                            '_id': None,
                                                            '_type':'link'
                                                         }

_id_body  = { 'query': { 'ids' : { 'type': None, 'values': [None] } } }

_lnk_body = {'query':{'bool':{'must':[{'term':{'from_ID': None}}, {'term':{'to_ID':None}}]}}};
#-------------------------------------------------------------------------------------------------------------------------------------------------
#-FUNCTIONS---------------------------------------------------------------------------------------------------------------------------------------

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
    if results['hits']['total'] == 0: # This is still the outdated ES version
        return False
    return True;

def exists(from_ID,to_ID,client,index):
    body = copy(_lnk_body);
    body['query']['bool']['must'][0]['term']['from_ID'] = from_ID;
    body['query']['bool']['must'][1]['term']['to_ID']   = to_ID;
    results = client.search(index=index,body=body);
    if (_GWS and results['hits']['total']==0) or (not _GWS and results['hits']['total']['value']==0):
        return False;
    return results['hits']['hits'][0]['_id'];

def update(doc):
    body = copy(_upd_body);
    body['_index']    = 'manual-links';
    body['_id']       = doc['_id'];
    source            = {};#doc['_source'];
    source['checked'] = True;
    body['_source']   = {'doc':source};
    print(body);
    return body;

def delete(doc):
    body = copy(_del_body);
    body['_id'] = doc['_id'];
    print(body);
    return body;

def check(doc):
    client    = ES([_ADDRESS],scheme=_SCHEME,port=_PORT,timeout=_TIMEOUT);
    client_id = ES(['search.gesis.org/es-config/'],scheme='http',port=80,timeout=60);
    source    = doc['_source'];
    status    = '';
    if (not 'annotation_time' in source) or (not valid_date(source['annotation_time'])):    # annotation time must be valid date in the past
        status += '/ no or invalid annotation_time (remember timezone), should be ISO 8601 or similar, future dates will be rejected /';
    else:
        source['annotation_time'] = valid_date(source['annotation_time']);
    if (not 'annotator' in source) or (not isinstance(source['annotator'],str)):    # annotator must be string
        status += '/ no annotator or not string /';
    if (not 'correct' in source) or (not source['correct'] in [0,1,2]):    # correct must be 0, 1 or 2
        status += '/ correct needs to be 0, 1, or 2 /';
    if ('utilized' in source) and (not source['utilized'] in [0,1,2]):     # correct must be 0, 1 or 2
        status += '/ utilized needs to be 0, 1, or 2 /';
    if ('correct' in source and 'utilized' in source) and (source['correct']!=None and source['utilized']!=None) and (not (source['correct'],source['utilized'],) in [(0,0,),(1,0,),(1,1,),(1,2,),(2,0,),(2,2,)]): # not all combinations make sense
        status += '/ combination of correct and utilized makes no sense /';
    if (not 'from_ID' in source) or (not valid_id(source['from_ID'],'publication',client_id)):    # from_ID must be an id in gws
        status += '/ no or nonexistant from_ID /';
    if (not 'to_ID' in source) or (not valid_id(source['to_ID'],'research_data',client_id)):    # from_ID must be an id in gws
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
    client   = ES([_ADDRESS],scheme=_SCHEME,port=_PORT,timeout=_TIMEOUT);
    page     = client.search(index=_in_index,scroll='2m',size=100,body=_scr_body);
    sid      = page['_scroll_id'];
    size     = float(page['hits']['total']) if _GWS else float(page['hits']['total']['value']);
    returned = len(page['hits']['hits']);#size;
    page_num = 0;
    while returned > 0:
        for doc in page['hits']['hits']:
            yield check(doc);
            if _DELETE:
                yield delete(doc);
            else:
                yield update(doc);
        scroll_tries = 0;
        while scroll_tries <= _TRIES:
            try:
                page      = client.scroll(scroll_id=sid, scroll='2m');
                returned  = len(page['hits']['hits']);
                page_num += 1;
            except Exception as exception:
                print(exception);
                print('WARNING: Some problem occured while scrolling. Sleeping for 3s and retrying...');
                scroll_tries += 1;
                time.sleep(3); continue;
            break;
#-------------------------------------------------------------------------------------------------------------------------------------------------
#-SCRIPT------------------------------------------------------------------------------------------------------------------------------------------

_client = ES([_ADDRESS],scheme=_SCHEME,port=_PORT,timeout=_TIMEOUT);

while True:

    try:
        i = 0;
        for success, info in bulk(_client,get_links()):
            i += 1;
            if not success:
                print('A document failed:', info['index']['_id'], info['index']['error']);
            elif i % 10000 == 0:
                print(i);
    except Exception as exception:
        print(exception);
        time.sleep(_SLEEP_);
    time.sleep(_SLEEP);
    print(datetime.datetime.now().isoformat(),end='\r');
#-------------------------------------------------------------------------------------------------------------------------------------------------
