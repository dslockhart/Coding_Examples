import boto3
import os
import json
import logging
from logging.config import fileConfig

fileConfig('logging_config.ini')
logger = logging.getLogger(__name__)

class IngestData:
    """
    Pipeline to read local json files and push to dynamodb warehouse
    """

    def __init__(self, **kwargs):
        self.aws_ak = kwargs.get('aws_access_key_id', None)
        self.aws_sak = kwargs.get('aws_secret_access_key', None)
        self.table_name = kwargs.get('table_name', 'transaction')
        self.file = kwargs.get('file', 'data.json')
        self.key = kwargs.get('key', 'transactions')

    def build_client(self):
        """Connect to DynamoDB"""
        DB = boto3.resource('dynamodb', aws_access_key_id=self.aws_ak,
                            aws_secret_access_key=self.aws_sak)
        table = DB.Table(self.table_name)
        return table

    def read_data(self):
        """Read local json file"""
        logger.info('Reading data from {}'.format(self.file))
        with open(self.file) as json_file:
            json_data = json.load(json_file)
        return json_data

    def put_data(self, data, table):
        """Push data to AWS DynamoDB"""
        for field in data[self.key][:5]:
            customerid = field['customerId']
            table.put_item(Item=field)
            logger.info('{} loaded'.format(customerid))
        logger.info('Loading finished')

    def main(self):
        """Run pipeline"""
        logger.info('Starting pipeline')
        table = self.build_client()
        # self.truncate_dynamo(table)
        # time.sleep(60)
        json_data = self.read_data()
        self.put_data(json_data, table)


if __name__ == '__main__':
    os.environ['AWS_DEFAULT_REGION'] = 'eu-west-2'
    table_name = 'transaction'
    aws_access_key_id = 'xx'
    aws_secret_access_key = 'xx'
    file = 'data.json'
    key = 'transactions'

    obj_args = {'table_name': table_name, 'file': file, 'key': key, 'aws_access_key_id': aws_access_key_id,
                'aws_secret_access_key': aws_secret_access_key}

    obj = IngestData(**obj_args)
    obj.main()

