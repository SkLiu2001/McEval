import sys
from tqdm import tqdm
import pandas as pd
import os
import json
from odps import ODPS
from odps.tunnel import TableTunnel
from odps.models import Column, Partition, Schema,TableSchema
import pyarrow as pa
import multiprocessing
# 获取当前脚本的绝对路径, 二级目录则连续调用两次dirname
script_dir = os.path.dirname(os.path.abspath(__file__))
root_path = os.path.dirname(script_dir)


class ODPSExecutor(object):

    ACCESS_ID = ''
    ACCESS_KEY = ''
    ENDPOINT = ''  # 办公网

    def __init__(self, app_id=ACCESS_ID, app_key=ACCESS_KEY, endpoint=ENDPOINT, project=None, table_name=None):
        assert project is not None, "project should not be None!!!"
        self.n_processor = multiprocessing.cpu_count()
        self.project_handler = ODPS(app_id, app_key, project, endpoint=endpoint)
        if table_name is not None:
            self.table = self.project_handler.get_table(table_name)
        else:
            self.table = None

    def exeSQL(self, sql_cmd):
        # 在项目中执行SQL
        try:
            with self.project_handler.execute_sql(sql_cmd).open_reader() as reader:
                for record in reader:
                    yield record
        except Exception as e:
            print("read odps error")
            print(str(e))

    def create_table(self, table_name, schema,  lifecycle, if_not_exists=True):
        assert table_name is not None, "table_name should not be None!!!"
        assert schema is not None, "schema should not be None!!!"
        self.project_handler.create_table(table_name, schema, lifecycle, if_not_exists=if_not_exists)
        print(f"Table {table_name} successfully created.")
        self.table = self.project_handler.get_table(table_name)

    def write_from_list(self, list_data, table_name, partition):
        assert table_name is not None, "table_name should not be None!!!"
        assert partition is not None, "partition should not be None!!!"
        t = self.project_handler.get_table(table_name)
        save_data_list = []
        for data in list_data:
            temp_list_ = []
            for key, value in data.items():
                if key == "meta_data":
                    temp_list_.append(json.dumps(value))
                else:
                    temp_list_.append(value)
            save_data_list.append(temp_list_)
        with t.open_writer(partition=partition, create_partition=True) as writer:
            writer.write(save_data_list)

    def write_from_df(self, df, partition):
        assert self.table is not None, "No ODPSTable to write!!!"
        assert partition is not None, "partition should not be None!!!"
        tunnel = TableTunnel(self.project_handler)
        session = tunnel.create_upload_session(self.table.name, partition_spec=partition)
        with session.open_arrow_writer(0) as writer:
            batch = pa.RecordBatch.from_pandas(df)
            writer.write(batch)
        session.commit([0])

    def download_to_jsonl(self, partition, save_path):
        assert partition is not None, "partition should not be None!!!"
        assert save_path is not None, "save_path should not be None!!!"
        # 创建一个下载器来获取结果
        tunnel = TableTunnel(self.project_handler)
        download_session = tunnel.create_download_session(self.table.name, partition_spec=partition)
        # 准备写入文件
        print(f'Downloading data from {self.table.name}/{partition}...')
        with open(save_path, 'w', encoding='utf-8') as output_file:
            with download_session.open_record_reader(0, download_session.count) as reader:
                for record in tqdm(reader, total=reader.count):
                    output_file.write(json.dumps(dict(record), ensure_ascii=False) + '\n')

        print(f'Data has been saved to {save_path}')


if __name__ == "__main__":
    project_name = 'future_xingchen'
    table_name = 'mcevl_demo'
    o = ODPSExecutor(project=project_name, table_name=table_name)
    # data_path = 'data'
    # 建表
    # 创建schema对象
    # schema = TableSchema(
    # columns=[
    #     Column(name='text', type='string'),                            # 源代码文本
    #     Column(name='claude35_sonnet_created_question', type='string'), # 问题描述
    #     Column(name='claude35_sonnet_check', type='string'),            # 检查函数
    #     Column(name='instruction', type='string')                       # 指令
    # ],
    # partitions=[
    #     Partition(name='ds', type='string')                             # 分区字段
    # ])
    def reorder_dict(item):
    # 定义字段顺序
        field_order = [
            'task_id',
            'prompt',
            'canonical_solution',
            'test',
            'entry_point',
            'signature',
            'docstring',
            'instruction',
            'level',
            'raw_generation',
            'module'
        ]
        
        # 创建新的有序字典
        return {field: item.get(field) for field in field_order if field in item}

    # 处理整个数据列表
    data_path = 'to_test/data/data370.jsonl'
    with open(data_path, 'r', encoding='utf-8') as f:
        # 读取并重新排序每个字典
        data_list = [reorder_dict(json.loads(line)) for line in f]

        #print(type(data_list[0]['raw_generation']))
    # for idx, data in enumerate(data_list):
    #     # 对键值进行排序
        
        
    # 上传数据
    partition = 'ds=370_v3'
    o.write_from_list(data_list, table_name=table_name ,partition=partition)
    
    # 以dataframe形式写表
    # datasets = ('RefGPT-Code-bg', 'RefGPT-Code-cr', 'RefGPT-Code-ds')
    # df = pd.DataFrame()
    # for ds in datasets:
    #     data_dir = os.path.join(data_path, ds, 'data')
    #     en_json_dir = os.path.join(data_dir, 'en.jsonl')
    #     zh_json_dir = os.path.join(data_dir, 'zh.jsonl')
    #
    #     df_en = pd.read_json(en_json_dir, lines=True)
    #     df_zh = pd.read_json(zh_json_dir, lines=True)
    #     df = pd.concat([df, df_en, df_zh], axis=0, ignore_index=True)

    # splits = ('train', 'val', 'test')
    # df = pd.DataFrame()
    # for split in splits:
    #     df_temp = pd.read_csv(os.path.join(data_path, f'{split}.csv'))
    #     # 对df_temp的列重命名
    #     df_temp = df_temp.rename(columns={'hash': 'id',
    #                                       'diff': 'text',
    #                                       'message': 'commit_msg',
    #                                       'diff_languages': 'program_lang',
    #                                       'project': 'project_name',
    #                                       'split': 'data_split'})
    #     df_temp['source'] = 'commitbench'
    #     df = pd.concat([df, df_temp])
    # df['unused_1'] = ''
    # df['unused_2'] = ''
    # df['unused_3'] = ''

    # df = df.reset_index(drop=True)
    # df['id'] = df.index
    #
    # o.write_from_df(df, partition=partition)
    # o.write_from_list(df.to_dict(orient='records'), table_name, partition=partition)
    #
    # 下载
    # o.download_to_jsonl(partition=partition, save_path='taogpt_refgpt_full_chatv3.0_taskl1.jsonl')
