from pyhealth.datasets import MIMIC3Dataset
from pyhealth.medcode import InnerMap  # 所有医疗代码系统的 基抽象类(不能实例化)，通过 .load(vocabulary) 实例化为 特定 医疗代码系统
from tqdm import tqdm
import pandas as pd
import json
import os


class DataMapper:
    def __init__(self, input_dir = './dataset/MIMICIII_data/', output_dir = './dataset_processed/MIMICIII_data/'):
        """
        初始化数据映射类
        :param input_dir: 输入文件夹 路径
        :param output_file: 输出文件夹 路径
        """
        self.input_dir = input_dir
        self.output_dir = output_dir

        # medcode 映射对象
        self.icd9cm = InnerMap.load("ICD9CM")  # 诊断
        self.icd9proc = InnerMap.load("ICD9PROC") # 手术
        self.ndc = InnerMap.load("NDC") # 处方
        # self.mimic3_itemid = InnerMap.load("")  # 实验室检查数据

        # 需要保留的字段
        self.patients_field = ['SUBJECT_ID', 'EXPIRE_FLAG']
        self.admission_field = ['SUBJECT_ID', 'HADM_ID', 'ADMITTIME', 'DISCHTIME', 'DEATHTIME']
        self.procedures_field = ['SUBJECT_ID', 'HADM_ID', 'ICD9_CODE']

    def _load_csv(self, filename):
        """
        加载CSV文件数据
        :param filename: CSV文件名
        :return: DataFrame
        """
        file_path = os.path.join(self.input_dir, filename)
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"文件 {file_path} 不存在。")
        return pd.read_csv(file_path)

    def mapper_table(self, filename, output_filename, reserved_field, n_rows=5, code_map = None):
        """
        仅保留 CSV文件中的 SUBJECT_ID 和 EXPIRE_FLAG 字段， 并存储为 pkl 文件
        :param filename: 输入的CSV文件名
        :param output_filename: 输出的pkl文件名
        :param reserved_field: 需要保留的字段
        :param n_rows: 要显示的前几行数据
        """
        # 加载 CSV文件 并 过滤无关字段
        df = self._load_csv(filename)[reserved_field]

        # code 映射
        if code_map:
            pass
        
        # 排序
        if 'SEQ_NUM' in reserved_field:
            df = df.sort_values(by=['SUBJECT_ID', 'HADM_ID', 'SEQ_NUM'])
        elif 'HADM_ID' in reserved_field:
            df = df.sort_values(by=['SUBJECT_ID', 'HADM_ID'])
        else:
            df = df.sort_values(by='SUBJECT_ID')

        # 输出前几行数据
        print(f"前{n_rows}行数据：")
        print(df.head(n_rows))
        
        # 确保输出目录存在
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
        
        # 保存为pkl文件
        output_path = os.path.join(self.output_dir, output_filename)
        df.to_pickle(output_path)
        print(f"数据已保存为 {output_path}")

    def read_pkl(self, filename):
        """
        读取指定的pkl文件，并打印前10行数据和数据总条数
        :param filename: 输入的pkl文件名
        """
        # 构建文件路径
        file_path = os.path.join(self.output_dir, filename)
        
        # 检查文件是否存在
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"文件 {file_path} 不存在。")
        
        # 读取pkl文件
        df = pd.read_pickle(file_path)
        
        # 打印前10行数据
        print(f"前20行数据：")
        print(df.head(20))
        
        # 打印数据总条数
        print(f"数据总条数：{len(df)}")

if __name__ == "__main__":
    mapper = DataMapper()

    # # patients 数据提取
    # mapper.mapper_table(filename = 'PATIENTS.csv', output_filename = 'patients_filtered.pkl', reserved_field = mapper.patients_field)
    # mapper.read_pkl('patients_filtered.pkl')

    # # admissions 数据的提取
    # mapper.mapper_table(filename = 'ADMISSIONS.csv', output_filename = 'admissions_filtered.pkl', reserved_field = mapper.admission_field)
    # mapper.read_pkl('admissions_filtered.pkl')
    print(mapper.icd9proc.lookup('326'))
    print(mapper.icd9proc.lookup('3348'))
    print(mapper.icd9proc.lookup('3404'))
