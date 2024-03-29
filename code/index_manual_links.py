#-IMPORTS-----------------------------------------------------------------------------------------------------------------------------------------
import sys, os
import time
import json
from copy import deepcopy as copy
from elasticsearch import Elasticsearch as ES
from elasticsearch.helpers import parallel_bulk as bulk
#-------------------------------------------------------------------------------------------------------------------------------------------------
#-GLOBAL OBJECTS----------------------------------------------------------------------------------------------------------------------------------
_infiles = sys.argv[1:];

_header = ['from_ID','to_ID','correct','utilized','annotation_time','annotator','from_SubID','to_SubID'];

_body = { '_op_type': 'index',
          '_index':   'manual-links',
          '_type':    'link',
          '_id':      None,
          '_source':  {field:None for field in _header },
        }; #TODO: This is currently made for the outdated GWS-server ES version
#-------------------------------------------------------------------------------------------------------------------------------------------------
#-FUNCTIONS---------------------------------------------------------------------------------------------------------------------------------------
def get_links(infile):
    IN    = open(infile,'r');
    links = json.load(IN);
    for i in range(len(links['links'])):
        body                   = copy(_body);
        body['_id']            = links['links'][i]['from_ID']+'-->'+links['links'][i]['to_ID'];
        body['_source'] = links['links'][i];
        print(body);
        yield body;
    IN.close();
#-------------------------------------------------------------------------------------------------------------------------------------------------
#-SCRIPT------------------------------------------------------------------------------------------------------------------------------------------

#client = ES(['svko-skg.gesis.intra/'],scheme='http',port=9200,timeout=60);
client = ES(['search.gesis.org/es-config/'],scheme='http',port=80,timeout=60);

for infile in _infiles:
    print(infile);
    i = 0;
    for success, info in bulk(client,get_links(infile)):
        i += 1;
        if not success:
            print('A document failed:', info['index']['_id'], info['index']['error']);
        elif i % 10000 == 0:
            print(i);
#-------------------------------------------------------------------------------------------------------------------------------------------------
