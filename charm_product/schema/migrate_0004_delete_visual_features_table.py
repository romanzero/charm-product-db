import boto3

from charm_product.util import get_table_name


def migrate():
    client = boto3.client('dynamodb')

    client.delete_table(
        TableName=get_table_name('product_visual_features'),
    )


if __name__ == '__main__':
    migrate()
