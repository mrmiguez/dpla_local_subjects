import shutil
import sys
import json
from dpla_local_map import dpla_local_map


def rec_gen(source_file):
    with open(source_file + ".bak") as f:
        recs = json.load(f)
        for rec in recs:
            yield rec


def sub_gen(rec):
    try:
        for sub in rec['sourceResource']['subject']:
            yield sub
    except KeyError:
        pass


shutil.move(sys.argv[1], sys.argv[1] + ".bak")
out = open(sys.argv[1], 'a', encoding='utf8', newline='\n')
count = 0
for rec in rec_gen(sys.argv[1]):
    for sub in sub_gen(rec):
        if sub['name'] in dpla_local_map.keys():
            rec['sourceResource']['subject'].append({'name': dpla_local_map[sub['name']][0]})
            break
    out.write(json.dumps(rec) + '\n')
