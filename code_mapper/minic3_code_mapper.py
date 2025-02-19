from pyhealth.medcode import InnerMap
from pyhealth.datasets import MIMIC3Dataset
from pyhealth.tasks import mortality_prediction_time_series_mimic3_fn
from tqdm import tqdm
import json


class Mapping_EHR_Code:
    def __init__(self) -> None:
        dataset = MIMIC3Dataset(
        root='./dataset/MIMICIII_data/',
        tables=["DIAGNOSES_ICD", "PROCEDURES_ICD", "PRESCRIPTIONS"],  # 加载的表格
        code_mapping={
            # "ICD9CM": "CCSCM",
            # "ICD9PROC": "CCSPROC",
            "NDC": ("ATC", {"target_kwargs": {"level": 3}})  # 定义药物编码标准，限制分类到 第三层
        },
        dev=True,  # 开发模式（仅使用数据的一小部分）
        refresh_cache=True  # 刷新缓存
    )
        
        print('加载成功：', type(dataset))
        
        # 构建映射表
        # sample_dataset = dataset.set_task_ts(mortality_prediction_time_series_mimic3_fn)
        # code_vocs = sample_dataset.code_vocs  # 映射表

        # self.samples = sample_dataset.samples  # 样本 type: list[dict]


        code_vocs = dataset.code_vocs
        self.sample = self.__load_minic3()  # 存放加载的数据


        # 加载 映射表
        self.diagnosis_map_table = InnerMap.load(code_vocs['conditions']) 
        self.procedures_map_table = InnerMap.load(code_vocs['procedures'])
        self.prescriptions_map_table = InnerMap.load(code_vocs['drugs'])


    def __load_minic3(self):
        """
        加载 minic3 部分数据，返回
        """
        train_dataset, val_dataset, test_dataset = load_ehr_dataset('mimic3', 42)  # 读取 .pkl文件数据，并划分
        res = list()  # 存储可用数据：visit次数 至少为 2
        for patient in test_dataset:
            if(len(patient['conditions']) > 1):
                res.append(patient)
        
        print(len(res))
        # 只保留这几个 key: conditions, procedures, delta_days, drugs
        keys_to_keep = ['patient_id', 'conditions', 'procedures', 'drugs']
        for i, data in enumerate(res):
            res[i] = {k: data[k] for k in keys_to_keep if k in data}
        print(res[0])

        return res  # list[dict]
    

    def transform_to_nl(self, warning_msg = False):
        """
        对 minic3 数据集中的 code 进行到 nl 的映射 （很少比例的数据 映射不成功）,
        存储为 json 文档
        """
        for index, sample in tqdm(enumerate(self.sample), desc='Code mapping processing:'):
            for i in range(len(sample['conditions'])):
                for j in range(len(sample['conditions'][i])):
                    try:
                        sample['conditions'][i][j] = self.diagnosis_map_table.lookup(sample['conditions'][i][j])
                    except Exception as e:
                        if warning_msg:
                            print('Warning: id 为 ',sample['patient_id'], '的病人 could not be found')

            
            for i in range(len(sample['procedures'])):
                for j in range(len(sample['procedures'][i])):
                    try:
                        sample['procedures'][i][j] = self.procedures_map_table.lookup(sample['procedures'][i][j])
                    except Exception as e:
                        if warning_msg:
                            print('Warning: id 为 ',sample['patient_id'], '的病人 could not be found')

        
            for i in range(len(sample['drugs'])):
                for j in range(len(sample['drugs'][i])):
                    try:
                        sample['drugs'][i][j] = self.prescriptions_map_table.lookup(sample['drugs'][i][j])
                    except Exception as e:
                        if warning_msg:
                            print('Warning: id 为 ',sample['patient_id'], '的病人 could not be found')

            if index%50 == 4:
                with open('minic3_complete_nl.json', 'w', encoding='utf-8') as json_file:
                    json.dump(self.sample, json_file, ensure_ascii=False, indent=4)

        with open('minic3_complete_nl.json', 'w', encoding='utf-8') as json_file:
                json.dump(self.sample, json_file, ensure_ascii=False, indent=4)

                

# 读取 mimic3数据集，然后转化为 json 文件持久化
if __name__ == '__main__':
    import os
    #os.chdir(os.path.dirname(os.path.abspath(__file__))) # 默认为项目目录

    print(os.getcwd())
    m = Mapping_EHR_Code()


    # m.transform_to_nl(True)

"""
samples[0]:
{'visit_id': '130744', 'visit_sequence': [Visit 130744 from patient 103 with 58 events from tables ['DIAGNOSES_ICD', 'PROCEDURES_ICD', 'PRESCRIPTIONS']], 
'visit_sequence_codes': [['1983', '431', '1623', '486', '4019', 'V1582', '78321', '2559', '0159', 'C08C', 'C07A', 'C09A', 'B05X', 'A12A', 'N02B', 'H02A', 'A02B', 'J01M', 'C02D', 'V06D', 'N05B', 'J01D', 'B01A', 'N07B', 'R06A', 'N02A']],
 'patient_id': '103', 'label': [1]}
"""