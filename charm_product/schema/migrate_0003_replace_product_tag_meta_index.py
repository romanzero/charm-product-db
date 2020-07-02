import boto3
import time

from charm_product.util import get_table_name


RETRY_ATTEMPTS = 10
RETRY_DELAY = 3


def migrate():
    client = boto3.client('dynamodb')

    def boto_do_retry(f):
        """
        Boto Client will throw errors while resources are updating.
        Use this function to catch and retry those errors until the operation
        succeeds.
        """
        for i in range(RETRY_ATTEMPTS):
            try:
                f()
                return
            except (
                client.exceptions.LimitExceededException,
                client.exceptions.ResourceInUseException
            ):
                if i == (RETRY_ATTEMPTS - 1):
                    raise
                time.sleep(RETRY_DELAY)

    client.update_table(
        TableName=get_table_name('product_tag'),
        GlobalSecondaryIndexUpdates=[{
            'Delete': {'IndexName': 'update_product_meta_idx'}
        }],
    )
    boto_do_retry(lambda: client.update_table(
        TableName=get_table_name('product_tag'),
        AttributeDefinitions=[
            {
                'AttributeName': 'store_product_url',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'update_product_meta',
                'AttributeType': 'N'
            },
        ],
        GlobalSecondaryIndexUpdates=[
            {
                'Create': {
                    'IndexName': 'update_product_meta_idx',
                    'KeySchema': [
                        {
                            'AttributeName': 'store_product_url',
                            'KeyType': 'HASH'
                        },
                        {
                            'AttributeName': 'update_product_meta',
                            'KeyType': 'RANGE'
                        },
                    ],
                    'Projection': {
                        'ProjectionType': 'ALL',
                    },
                },
            },
        ],
    ))


if __name__ == '__main__':
    migrate()
