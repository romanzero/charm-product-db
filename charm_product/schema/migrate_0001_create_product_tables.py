import boto3

from charm_product.util import get_table_name


def migrate():
    client = boto3.client('dynamodb')

    client.create_table(
        TableName=get_table_name('product'),
        AttributeDefinitions=[
            # unique reference to a product
            # (hash of product URL for product)
            {
                'AttributeName': 'store_product_url',
                'AttributeType': 'S'
            },
            # UUID
            # - auto-generate when adding new products
            # - merge with matching "store product" UUIDs when running
            #   "mega products" pipeline
            {
                'AttributeName': 'product_uuid',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'brand_domain',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'store_domain',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'vendor_name',
                'AttributeType': 'S'
            },
            {
                # Set to 1 if product is available, otherwise do not set this key
                # (use a sparse index to reduce index size)
                'AttributeName': 'is_available',
                'AttributeType': 'N'
            },
        ],
        KeySchema=[
            {
                'AttributeName': 'store_product_url',
                'KeyType': 'HASH'
            }
        ],
        GlobalSecondaryIndexes=[
            # "Mega product" queries
            {
                'IndexName': 'product_uuid_idx',
                'KeySchema': [
                    {
                        'AttributeName': 'product_uuid',
                        'KeyType': 'HASH'
                    },
                    {
                        'AttributeName': 'is_available',
                        'KeyType': 'RANGE'
                    },
                ],
                'Projection': {
                    'ProjectionType': 'ALL',
                },
            },
            # Brand product queries
            {
                'IndexName': 'brand_domain_idx',
                'KeySchema': [
                    {
                        'AttributeName': 'brand_domain',
                        'KeyType': 'HASH'
                    },
                    {
                        'AttributeName': 'is_available',
                        'KeyType': 'RANGE'
                    },
                ],
                'Projection': {
                    'ProjectionType': 'ALL',
                },
            },
            # Store product queries
            {
                'IndexName': 'store_domain_idx',
                'KeySchema': [
                    {
                        'AttributeName': 'store_domain',
                        'KeyType': 'HASH'
                    },
                    {
                        'AttributeName': 'is_available',
                        'KeyType': 'RANGE'
                    },
                ],
                'Projection': {
                    'ProjectionType': 'ALL',
                },
            },
            # Look up products by store/vendor
            {
                'IndexName': 'store_vendor_idx',
                'KeySchema': [
                    {
                        'AttributeName': 'store_domain',
                        'KeyType': 'HASH'
                    },
                    {
                        'AttributeName': 'vendor_name',
                        'KeyType': 'RANGE'
                    },
                ],
                'Projection': {
                    'ProjectionType': 'KEYS_ONLY',
                },
            },
        ],
        BillingMode='PAY_PER_REQUEST',
    )

    client.create_table(
        TableName=get_table_name('product_visual_features'),
        AttributeDefinitions=[
            {
                'AttributeName': 'store_product_url',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'image_url',
                'AttributeType': 'S'
            },
        ],
        KeySchema=[
            {
                'AttributeName': 'store_product_url',
                'KeyType': 'HASH'
            },
            {
                'AttributeName': 'image_url',
                'KeyType': 'RANGE'
            },
        ],
        BillingMode='PAY_PER_REQUEST',
    )

    client.create_table(
        TableName=get_table_name('product_tag'),
        # tags include:
        # - "image_not_indexed"
        # - "run_megaproduct_pipeline"
        AttributeDefinitions=[
            {
                'AttributeName': 'store_product_url',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'tag',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'image_not_indexed',
                'AttributeType': 'N'
            },
            {
                'AttributeName': 'update_product_meta',
                'AttributeType': 'N'
            },
        ],
        KeySchema=[
            {
                'AttributeName': 'store_product_url',
                'KeyType': 'HASH'
            },
            {
                'AttributeName': 'tag',
                'KeyType': 'RANGE'
            },
        ],
        GlobalSecondaryIndexes=[
            {
                'IndexName': 'image_not_indexed_idx',
                'KeySchema': [
                    {
                        'AttributeName': 'store_product_url',
                        'KeyType': 'HASH'
                    },
                    {
                        'AttributeName': 'image_not_indexed',
                        'KeyType': 'RANGE'
                    },
                ],
                'Projection': {
                    'ProjectionType': 'KEYS_ONLY',
                },
            },
            {
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
                    'ProjectionType': 'KEYS_ONLY',
                },
            },
        ],
        BillingMode='PAY_PER_REQUEST',
    )


if __name__ == '__main__':
    migrate()
