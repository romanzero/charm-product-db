import botocore
import uuid
from boto3.dynamodb.conditions import Key

from charm_product.tag import ProductTag, set_product_tag, fetch_product_tags
from charm_product.util import PRODUCT_UUID_BLACKLIST, clean_product_url, get_table_name
from charm_product.validation import parse_store_product_data


def add_store_product(
    dynamodb,
    product_url,
    store_domain,
    is_available=True,
    **attrs
):
    product_table = dynamodb.Table(get_table_name('product'))

    store_product_url = clean_product_url(product_url)
    item_data = dict(
        store_product_url=store_product_url,
        full_store_product_url=product_url,
        store_domain=store_domain,
        # "is_available" is stored as a "number" in DynamoDB
        # (required to allow indexing)
        is_available=int(is_available),
        **attrs
    )

    item_data = parse_store_product_data(item_data)

    # Set "brand domain" if available for new products
    # (for existing products, this is set according to "product_uuid" by bulk
    # data processing job)
    if attrs.get('store_product_brand_domain'):
        item_data['brand_domain'] = attrs.get('store_product_brand_domain')

    try:
        # product does not yet exist in DB, assign a new product ID
        item_data['product_uuid'] = uuid.uuid4().hex

        product_table.put_item(
            Item=item_data,
            ConditionExpression='attribute_not_exists(store_product_url)'
        )
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] != 'ConditionalCheckFailedException':
            raise
        else:
            raise ValueError(f'Product with url "{store_product_url}" already exists')

    primary_image_url = None
    if item_data.get('image_urls'):
        primary_image_url = item_data['image_urls'][0]

    # Tag this store product for requiring indexing if it has an image
    if primary_image_url:
        set_product_tag(
            dynamodb,
            store_product_url, ProductTag.image_not_indexed,
            image_url=primary_image_url
        )
    # Tag this store product for requiring metadata update
    set_product_tag(dynamodb, store_product_url, ProductTag.update_product_meta)


def update_store_product(dynamodb, product_url, **attrs):
    product_table = dynamodb.Table(get_table_name('product'))

    store_product_url = clean_product_url(product_url)

    item_data = dict(
        full_store_product_url=product_url,
        **attrs
    )

    image_not_indexed = False
    update_product_meta = False

    old_item_data = product_table.get_item(
        Key={'store_product_url': store_product_url},
    ).get('Item')

    if not old_item_data:
        raise ValueError(f'Product with url "{store_product_url}" does not yet exist')

    old_primary_image_url = None
    if old_item_data.get('image_urls'):
        old_primary_image_url = old_item_data['image_urls'][0]
    new_primary_image_url = None
    if item_data.get('image_urls'):
        new_primary_image_url = item_data['image_urls'][0]

    if (
        new_primary_image_url is not None and
        # Assume query string does not affect image contents and compare image
        # URLs without query string component
        clean_product_url(new_primary_image_url) != clean_product_url(old_primary_image_url)
    ):
        image_not_indexed = True

    if (
        old_item_data.get('store_product_brand_domain') !=
        item_data.get('store_product_brand_domain')
    ):
        update_product_meta = True

    item_data = parse_store_product_data(item_data, new_item=False)

    update_expression = 'SET {}'.format(', '.join([
        f'{attr} = :{attr}' for attr in item_data
    ]))
    expression_attribute_values = {
        f':{attr}': value for attr, value in item_data.items()
    }
    product_table.update_item(
        Key={'store_product_url': store_product_url},
        UpdateExpression=update_expression,
        ExpressionAttributeValues=expression_attribute_values
    )

    # flag image for feature extraction and indexing
    if image_not_indexed:
        set_product_tag(
            dynamodb, store_product_url, ProductTag.image_not_indexed,
            image_url=new_primary_image_url
        )

    # flag product metadata to be updated (re-evaluate product "brand domain")
    if update_product_meta:
        set_product_tag(dynamodb, store_product_url, ProductTag.update_product_meta)


def get_store_product(dynamodb, product_url):
    product_table = dynamodb.Table(get_table_name('product'))
    store_product_url = clean_product_url(product_url)
    return product_table.get_item(Key={'store_product_url': store_product_url}).get('Item')


def delete_store_products(dynamodb, store_product_urls):
    product_table = dynamodb.Table(get_table_name('product'))
    tag_table = dynamodb.Table(get_table_name('product_tag'))

    with product_table.batch_writer() as batch:
        for sp_url in store_product_urls:
            batch.delete_item(Key={'store_product_url': sp_url})

    with tag_table.batch_writer() as batch:
        for tag in fetch_product_tags(dynamodb, store_product_urls):
            batch.delete_item(Key=dict(
                store_product_url=tag['store_product_url'],
                tag=tag['tag'],
            ))


def fetch_products_by_store(
    dynamodb,
    store_domain,
    is_available=True,
    **kwargs,
):
    key_expr = Key('store_domain').eq(store_domain)
    if is_available is not None:
        key_expr = key_expr & Key('is_available').eq(int(is_available))

    return _fetch_products(
        dynamodb, 'store_domain_idx', key_expr, **kwargs
    )


def fetch_products_by_brand(
    dynamodb,
    brand_domain,
    is_available=True,
    **kwargs,
):
    key_expr = Key('brand_domain').eq(brand_domain)
    if is_available is not None:
        key_expr = key_expr & Key('is_available').eq(int(is_available))

    return _fetch_products(
        dynamodb, 'brand_domain_idx', key_expr, **kwargs
    )


def fetch_products_by_product_uuid(
    dynamodb,
    product_uuid,
    is_available=True,
    **kwargs,
):
    key_expr = Key('product_uuid').eq(product_uuid)
    if is_available is not None:
        key_expr = key_expr & Key('is_available').eq(int(is_available))

    return _fetch_products(
        dynamodb, 'product_uuid_idx', key_expr, **kwargs
    )


def _fetch_products(
    dynamodb, index_name, key_expr,
    limit=None, only_attributes=None, consistent_read=False
):
    product_table = dynamodb.Table(get_table_name('product'))

    projection_expression = None
    if only_attributes is not None:
        # Always retrieve "product_uuid" so that blacklisted product UUIDs can
        # be filtered from results
        if 'product_uuid' not in only_attributes:
            fetch_attributes = list(only_attributes) + ['product_uuid']
        else:
            fetch_attributes = only_attributes
        projection_expression = ','.join(fetch_attributes)

    start_key = None
    while True:
        query_kwargs = {}
        query_kwargs['ConsistentRead'] = consistent_read
        if start_key is not None:
            query_kwargs['ExclusiveStartKey'] = start_key
        if projection_expression is not None:
            query_kwargs['ProjectionExpression'] = projection_expression
        if limit is not None:
            # Note, if we start using Filter Expressions on the results of
            # queries, "limit" will be applied before filtering results.
            # https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Query.html#Query.Limit
            query_kwargs['Limit'] = limit

        results = product_table.query(
            IndexName=index_name,
            KeyConditionExpression=key_expr,
            **query_kwargs
        )
        for item in results['Items']:
            # stored as a "number" in DynamoDB
            # (required to allow indexing)
            if 'is_available' in item:
                item['is_available'] = bool(item['is_available'])

            if item['product_uuid'] not in PRODUCT_UUID_BLACKLIST:
                if (
                    only_attributes is not None and
                    'product_uuid' not in only_attributes
                ):
                    # Do not include product UUID in results if not requested
                    del item['product_uuid']
                yield item

        if limit is not None:
            limit -= len(results['Items'])
            if limit <= 0:
                break

        start_key = results.get('LastEvaluatedKey')
        if start_key is None:
            break
