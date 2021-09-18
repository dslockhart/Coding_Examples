import boto3
from botocore.exceptions import ClientError
import io
import pandas as pd
import random
import logging
from logging.config import fileConfig
from typing import Tuple
from os import path
log_file_path = path.join(path.dirname(path.abspath(__file__)), 'logging_config.ini')
fileConfig(log_file_path)

from decorators.timer_decorator import timer_decorator
from prefix_lists.xxx_prefixes import xxx

class FixTypes:
    """Build a data_types config file by concatenating random data and inferring types"""
    def __init__(self, **kwargs):
        self.prefix = kwargs.get('prefix')
        self.bucket = kwargs.get('bucket')
        self.days_to_process = kwargs.get('days_to_process', 5)
        self.data_source = kwargs.get('data_source')
        self.logger = logging.getLogger(__name__)

    def build_conn(self) -> boto3.client:
        """Connect to S3"""
        try:
            s3 = boto3.resource('s3')
            bucket = s3.Bucket(self.bucket)
            self.logger.info('Connection built')
            return bucket
        except Exception as e:
            self.logger.error('Connection error: ' + str(e))


    def list_files(self, bucket) -> list:
        """Extract a list of objects in S3"""
        prefix_objs = bucket.objects.filter(Prefix=self.prefix)
        file_list = []
        try:
            for obj in prefix_objs:
                file_list.append(str(obj.key))
            self.logger.info('{} files found'.format(len(file_list)))
        except ClientError as e:
            self.logger.error('Expired token: ' + str(e))
        return file_list

    def build_concat(self, file_list: list, bucket) -> pd.DataFrame:
        """Build a pandas dataframe with concatenated data"""
        self.logger.info('Building dataframe')
        appended_data = []
        randlist = []
        prefix_objs = bucket.objects.filter(Prefix=self.prefix)
        # Randomise the available files
        for int in range(0, self.days_to_process):
            randint = random.randint(0, len(file_list)-1)
            randlist.append(file_list[randint])
        # Loop through S3 objects and extract data
        for obj in prefix_objs:
            if obj.key in randlist:
                body = obj.get()['Body'].read()
                file = pd.read_parquet(io.BytesIO(body))
                self.logger.info('Reading file {}'.format(obj.key))
                appended_data.append(file)
        dataframe = pd.concat(appended_data)
        return dataframe

    def _rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """To follow correct naming convention"""
        self.logger.info('Renaming columns')
        df.columns = df.columns.str.lower()
        df.columns = df.columns.str.strip('_')
        df.columns = df.columns.str.replace('%', 'pct', regex=False)
        df.columns = df.columns.str.replace(' ', '_', regex=False)
        df.columns = df.columns.str.replace('(', '_', regex=False)
        df.columns = df.columns.str.replace(')', '_', regex=False)
        df.columns = df.columns.str.replace('+', '_', regex=False)
        df.columns = df.columns.str.replace('-', '_', regex=False)
        df.columns = df.columns.str.replace('___', '_', regex=False)
        df.columns = df.columns.str.replace('__', '_', regex=False)
        df.columns = df.columns.str.replace('/', '_', regex=False)
        self.check_names(df)
        return df

    def check_names(self, df: pd.DataFrame) -> None:
        """Parse the column names for duplicates"""
        names = []
        for col_name in df.columns:
            if col_name not in names:
                names.append(col_name)
            else:
                self.logger.error(f'Column name [{col_name}] is duplicated')
                raise NameError(f'Column name [{col_name}] is duplicated')

    @staticmethod
    def output_original_types(df: pd.DataFrame):
        """Output a csv with original column types. Not used by default"""
        name_list = []
        type_list = []
        unique_list = []
        for col in df.columns:
            name_list.append(col)
        for col in df.dtypes:
            type_list.append(col)
        for col in df.columns:
            unique_list.append(df[col].unique())
        df0 = pd.DataFrame([name_list, type_list, unique_list]).transpose()
        df0.columns = ['col_name', 'col_dtype', 'unique']
        df0.to_csv('original_types.csv')

    @staticmethod
    def find_exemptions(df: pd.DataFrame) -> Tuple[list, list]:
        """
        Find all columns with date or string types
        :param df: concatenated dataframe
        :return: lists of date cols and string cols
        """
        datecols = []
        stringcols = []
        for col in df.columns:
            if 'date' in str(type(df[col][0])):
                datecols.append(col)
            if 'date' in col:
                datecols.append(col)
            if 'day' in col:
                datecols.append(col)
            if 'time' in col:
                datecols.append(col)
            elif 'name' in col:
                stringcols.append(col)
        return datecols, stringcols

    @staticmethod
    def fix_numeric(df: pd.DataFrame, exemptions: tuple) -> Tuple[pd.DataFrame, list]:
        """Apply to_numeric to all columns not in exemptions"""
        name_list1 = []
        datecols, stringcols = exemptions
        for col in df.columns:
            name_list1.append(col)
            if col not in datecols and col not in stringcols:
                df[col] = df[col].apply(pd.to_numeric, errors='ignore')
        return df, name_list1

    @staticmethod
    def build_output_df(fixed: tuple, datecols) -> pd.DataFrame:
        """Process dataframe for output types_info"""
        # Build dataframe from list of dtypes
        df, name_list = fixed
        type_list = []
        for col in df.dtypes:
            type_list.append(col)
        df1 = pd.DataFrame([name_list, type_list]).transpose()
        df1.columns = ['col_name', 'col_dtype']

        # Drop date columns from output
        for row in df1.iterrows():
            if row[1][0] in datecols:
                df1 = df1.drop(row[0])
        return df1

    def fix_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """Infer data types for all columns"""
        self.logger.info('Fixing Types')
        exemptions = self.find_exemptions(df)
        fixed = self.fix_numeric(df, exemptions)
        datecols, stringcols = exemptions
        df1 = self.build_output_df(fixed, datecols)
        return df1

    @staticmethod
    def format_dict(value: object) -> dict:
        """Build a dictionary and tidy up type names"""
        output_dict = {}
        output_dict['type'] = str(value[1]).replace('float64', 'float').replace('int64', 'int').replace\
                    ('object', 'string')
        output_dict['name'] = value[0]
        return output_dict

    def write_ti(self, df: pd.DataFrame) -> None:
        """Build a types info file"""
        self.logger.info('Writing types info.py')
        with open('{}_types_info.py'.format(self.data_source), 'w') as f:
            f.write('{}_types_info = ['.format(self.data_source))
            for _, value in df.iterrows():
                output_dict = self.format_dict(value)
                f.write(str(output_dict) + ',\n')
            f.write(']')

    def write_col_names(self, df) -> None:
        """Output a csv with all column names per data source"""
        cols = []
        for col in df.columns:
            cols.append(col)
        df_new = pd.DataFrame(cols)
        df_new.to_csv(f'{self.data_source}_cols.csv')

    @timer_decorator
    def main(self):
        """Run pipeline"""
        self.logger.info(f'Data source: {self.data_source}')
        s3buck = self.build_conn()
        file_list = self.list_files(s3buck)
        dataframe = self.build_concat(file_list, s3buck)
        rn_dataframe = self._rename_columns(dataframe)
        self.write_col_names(rn_dataframe)
        ft_dataframe = self.fix_types(rn_dataframe)
        self.write_ti(ft_dataframe)


if __name__ == '__main__':
    # Note that you can import a list of prefixes and put the below into a loop
    # Increasing days to process increases chances of catching rogue string cols
    obj_args = {'prefix': 'xxx',
                'bucket': 'xxx',
                'data_source': 'xxx',
                'days_to_process': 1}
    fixtypes = FixTypes(**obj_args)
    fixtypes.main()

