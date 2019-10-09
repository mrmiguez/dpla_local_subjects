import shutil
import sys
import json
from dpla_local_map import dpla_local_map


def rec_gen(source_file):
    """
    :param source_file: JSON file of SSDN records
    :return: Generator or records in source_file
    """
    with open(source_file + ".bak") as f:
        recs = json.load(f)
        for rec in recs:
            yield rec


def sub_gen(rec):
    """
    :param rec: JSON record
    :return: Generator of subjects in rec
    """
    try:
        for sub in rec['sourceResource']['subject']:
            yield sub
    except KeyError:
        pass


# create a backup of input file
shutil.move(sys.argv[1], sys.argv[1] + ".bak")
out = open(sys.argv[1], 'a', encoding='utf8', newline='\n')
for rec in rec_gen(sys.argv[1]):
    for sub in sub_gen(rec):
        '''
        check existing subjects against mapped terms 
        and make sure supplied subject isn't already
        in record
        '''
        if sub['name'] in dpla_local_map.keys() and dpla_local_map[sub['name']][0][0] not in [term['name'] for term in
                                                                                              rec['sourceResource'][
                                                                                                  'subject']]:
            if len(dpla_local_map[sub['name']]) > 1:
                for item in dpla_local_map[sub['name']]:
                    rec['sourceResource']['subject'].append({'name': item[0], "@id": item[1]})
                break
            else:
                rec['sourceResource']['subject'].append(
                    {'name': dpla_local_map[sub['name']][0][0], "@id": dpla_local_map[sub['name']][0][1]})
                break
    out.write(json.dumps(rec) + '\n')
