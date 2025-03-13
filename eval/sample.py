import os
import json
import random



data_path = 'to_test/data'
new_data_path = 'to_test/data10'
save_path = 'to_test/save'
langs = [x.split('.')[0] for x in os.listdir(data_path) if x.endswith('.jsonl')]
save_path = os.path.join(save_path, os.path.basename(data_path)+'.jsonl')
detail_save_path = os.path.join(save_path, os.path.basename(data_path)+'_detail.jsonl')
# print(save_path)
if os.path.exists(save_path):
    finish_langs = [x.split('\t')[0].strip().lower() for x in open(save_path, 'r').readlines()]
else:
    finish_langs = []

langs = [lang for lang in langs if (lang.lower() not in finish_langs)]
random.shuffle(langs)

# print(langs)
for lang in langs:
    print(lang)
    file_path = os.path.join(data_path, lang+'.jsonl')
    out_file_path = os.path.join(new_data_path, lang+'.jsonl')
    with open(file_path, 'r', encoding='utf-8') as f:
        data = [json.loads(x) for x in f.readlines()]
        new_data = data[:10]
    with open(out_file_path, 'w', encoding='utf-8') as f:
        for d in new_data:
            if 'canonical_solution' in d:
                d['raw_generation'] = [d['canonical_solution']] 
            f.write(json.dumps(d)+'\n')