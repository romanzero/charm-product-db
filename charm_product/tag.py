from boto3.dynamodb.conditions import Key
from enum import Enum, auto

from charm_product.util import clean_product_url, get_table_name


class ProductTag(Enum):
    image_not_indexed = auto()
    update_product_meta = auto()


def set_product_tag(dynamodb, store_product_url, product_tag, **attrs):
    tag_table = dynamodb.Table(get_table_name('product_tag'))
    store_product_url = clean_product_url(store_product_url)
    item = {
        'store_product_url': store_product_url,
        'tag': product_tag.name,
        product_tag.name: 1,
    }
    item.update(attrs)
    tag_table.put_item(Item=item)


def fetch_product_tags(dynamodb, store_product_urls, product_tag=None):
    tag_table = dynamodb.Table(get_table_name('product_tag'))

    index_name = None
    if product_tag is not None:
        index_name = f'{product_tag.name}_idx'

    for sp_url in store_product_urls:
        sp_url = clean_product_url(sp_url)
        start_key = None
        while True:
            key_expr = Key('store_product_url').eq(sp_url)

            query_kwargs = {}
            if start_key is not None:
                query_kwargs['ExclusiveStartKey'] = start_key
            if index_name is not None:
                query_kwargs['IndexName'] = index_name

            results = tag_table.query(
                KeyConditionExpression=key_expr,
                **query_kwargs
            )
            for item in results['Items']:
                yield item

            start_key = results.get('LastEvaluatedKey')
            if start_key is None:
                break


def delete_product_tags(dynamodb, product_tag, store_product_urls):
    tag_table = dynamodb.Table(get_table_name('product_tag'))

    with tag_table.batch_writer() as batch:
        for sp_url in store_product_urls:
            sp_url = clean_product_url(sp_url)
            batch.delete_item(Key=dict(
                store_product_url=sp_url,
                tag=product_tag.name,
            ))
