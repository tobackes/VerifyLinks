import sys
import time
import json
from elasticsearch import Elasticsearch as ES

_mapping = sys.argv[1];
_name    = sys.argv[2];

index_name = _name+'-'+time.ctime(time.time()).replace(' ','-').replace(':','').lower();

client = ES(['search.gesis.org/es-config/'],scheme='http',port=80,timeout=60);

IN      = open(_mapping);
mapping = json.load(IN);
IN.close();

indices = set(client.indices.get(_name+'-*')) & set(client.indices.get_alias(_name+"*"));
for index in indices:
    if index != _name:
        print('...deleting old index', index);
        client.indices.delete(index=index, ignore=[400, 404]);

response = client.indices.create( index=index_name, body=mapping );

print('created new index', index_name);
if 'acknowledged' in response:
    if response['acknowledged'] == True:
        print("INDEX MAPPING SUCCESS.");
elif 'error' in response:
    print("ERROR:", response['error']['root_cause']);
    print("TYPE:", response['error']['type']);

client.indices.put_alias(index=index_name, name=_name);
print('added alias "',_name,'" to index',index_name);