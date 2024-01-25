

from boto3 import client

# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/client/query.html

class DynamoAPI:

    _table = None
    _client = client(
        'dynamodb',aws_access_key_id='yyyy', aws_secret_access_key='xxxx', region_name='***'
    )

    @classmethod
    def find(cls, exprs: list = [], filters: dict = {}):
        pass

    @classmethod
    def find_one(cls, exprs: list = [], filters: dict = {}):
        pass

    @classmethod
    def update(cls, ):