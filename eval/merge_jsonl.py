import json
import os

input_dir = 'to_test/data10'
output_file = 'to_test/data/data370.jsonl'
exclude_langs = ['HTML', 'Markdown', 'JSON']

langs = [x.split('.')[0] for x in os.listdir(input_dir) if x.endswith('.jsonl')]
langs = [lang for lang in langs if (lang not in exclude_langs)]
print(langs)

all_data = []
for lang in langs:
    input_file_path = os.path.join(input_dir, lang+'.jsonl')
    with open(input_file_path, 'r') as f:
        items = [json.loads(x) for x in f.readlines() if x]
        if "task_id" not in items[0]:
            print(f"{lang}don't have task_id")
        if "prompt" not in items[0]:
            print(f"{lang}don't have prompt")
        if 'canonical_solution' not in items[0]:
            print(f"{lang}don't have canonical")
        if "test" not in items[0]:
            print(f"{lang}don't have test")
            for idx, item in enumerate(items):
                items[idx]['test'] = ''
        if "entry_point" not in items[0]:
            print(f"{lang}don't have entry_point")
            for idx, item in enumerate(items):
                items[idx]['entry_point'] = ''
        if "signature" not in items[0]:
            print(f"{lang}don't have signature")
            for idx, item in enumerate(items):
                items[idx]['signature'] = ''
        if "docstring" not in items[0]:
            print(f"{lang}don't have docstring")
            for idx, item in enumerate(items):
                items[idx]['docstring'] = ''
        if "raw_generation" not in items[0]:
            print(f"{lang}don't have raw_generation")
        if "module" not in items[0]:
            print(f"{lang}don't have module")
            for idx, item in enumerate(items):
                items[idx]['module'] = ''
            
        all_data.extend(items)
print(len(all_data))
with open(output_file, 'w', encoding='utf-8') as f:
    for item in all_data:
        f.write(json.dumps(item,ensure_ascii=False)+'\n')