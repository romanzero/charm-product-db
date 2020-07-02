import boto3
import uuid
import moto
import pytest
from ciso8601 import parse_datetime as parse_dt

from charm_product.product import (
    clean_product_url,
    add_store_product,
    update_store_product,
    get_store_product,
    delete_store_products,
    fetch_products_by_brand,
    fetch_products_by_store,
    fetch_products_by_product_uuid,
)
from charm_product.tag import ProductTag, delete_product_tags, fetch_product_tags
from charm_product.util import get_table_name


def update_product_attribute(dynamodb, product_url, attr, value):
    """
    Define separate function to update product table data without using the API.
    The API does not support updating certain attributes that are managed
    internally and by separate by batch processes.
    """
    product_table = dynamodb.Table(get_table_name('product'))
    store_product_url = clean_product_url(product_url)
    update_expression = f'SET {attr} = {value}'
    product_table.update_item(
        Key={'store_product_url': store_product_url},
        UpdateExpression=update_expression,
    )


@pytest.fixture()
def input_product_data():
    return [
        dict(
            product_url='https://waffles.food/product/waffles',
            store_domain='waffles.food',
            title='Waffles',
            vendor_name='waffle co',
            store_product_brand_domain='waffles.food',
            image_urls=['https://waffles.food/images/waffles'],
            scraper_type='generic_scraper',
            first_scraped_at=parse_dt('2020-06-01T00:00:01+00:00'),
            last_scraped_at=parse_dt('2020-06-01T00:00:01+00:00'),
        ),
        dict(
            product_url='https://waffles.food/product/extra-waffles',
            store_domain='waffles.food',
            title='Extra Waffles',
            vendor_name='waffle co',
            store_product_brand_domain='waffles.food',
            image_urls=['https://waffles.food/images/so-many-waffles'],
            scraper_type='generic_scraper',
            first_scraped_at=parse_dt('2020-06-01T00:00:02+00:00'),
            last_scraped_at=parse_dt('2020-06-01T00:00:02+00:00'),
        ),
        dict(
            product_url='https://waffles.food/product/express-shipping',
            store_domain='waffles.food',
            title='Express Shipping',
            vendor_name='waffle co',
            store_product_brand_domain='waffles.food',
            scraper_type='generic_scraper',
            first_scraped_at=parse_dt('2020-06-01T00:00:03+00:00'),
            last_scraped_at=parse_dt('2020-06-01T00:00:03+00:00'),
        ),
    ]


@pytest.fixture()
def expected_dynamodb_data():
    return [
        dict(
            store_product_url='waffles.food/product/extra-waffles',
            full_store_product_url='https://waffles.food/product/extra-waffles',
            store_domain='waffles.food',
            brand_domain='waffles.food',
            title='Extra Waffles',
            store_product_brand_domain='waffles.food',
            image_urls=['https://waffles.food/images/so-many-waffles'],
            is_available=True,
            vendor_name='waffle co',
            scraper_type='generic_scraper',
            first_scraped_at='2020-06-01T00:00:02+00:00',
            last_scraped_at='2020-06-01T00:00:02+00:00',
        ),
        dict(
            store_product_url='waffles.food/product/waffles',
            full_store_product_url='https://waffles.food/product/waffles',
            store_domain='waffles.food',
            brand_domain='waffles.food',
            title='Waffles',
            store_product_brand_domain='waffles.food',
            image_urls=['https://waffles.food/images/waffles'],
            is_available=True,
            vendor_name='waffle co',
            scraper_type='generic_scraper',
            first_scraped_at='2020-06-01T00:00:01+00:00',
            last_scraped_at='2020-06-01T00:00:01+00:00',
        ),
        dict(
            store_product_url='waffles.food/product/express-shipping',
            full_store_product_url='https://waffles.food/product/express-shipping',
            store_domain='waffles.food',
            brand_domain='waffles.food',
            title='Express Shipping',
            store_product_brand_domain='waffles.food',
            is_available=True,
            vendor_name='waffle co',
            scraper_type='generic_scraper',
            first_scraped_at='2020-06-01T00:00:03+00:00',
            last_scraped_at='2020-06-01T00:00:03+00:00',
        ),
    ]


@moto.mock_dynamodb2
def test_add_store_product(
    create_dynamodb_tables, input_product_data, expected_dynamodb_data
):
    dynamodb = boto3.resource('dynamodb')
    create_dynamodb_tables()

    for product in input_product_data:
        add_store_product(dynamodb, **product)

    actual_output = sorted(
        fetch_products_by_store(dynamodb, 'waffles.food'),
        key=lambda r: r['store_product_url'],
    )

    product_uuids = {
        p['store_product_url']: p.pop('product_uuid')
        for p in actual_output
    }

    # assert distinct product UUIDs
    assert len(set(product_uuids.values())) == 3

    # assert product UUIDs assigned to valid values
    for product_uuid in product_uuids.values():
        uuid.UUID(product_uuid)

    def sort_items(items):
        return sorted(items, key=lambda s: s['store_product_url'])

    assert sort_items(actual_output) == sort_items(expected_dynamodb_data)

    product_urls = [item['product_url'] for item in input_product_data]

    assert sorted([
        (t['store_product_url'], t['image_url'])
        for t in fetch_product_tags(dynamodb, product_urls, ProductTag.image_not_indexed)
    ]) == [
        ('waffles.food/product/extra-waffles', 'https://waffles.food/images/so-many-waffles'),
        ('waffles.food/product/waffles', 'https://waffles.food/images/waffles'),
    ]
    assert sorted([
        t['store_product_url']
        for t in fetch_product_tags(dynamodb, product_urls, ProductTag.update_product_meta)
    ]) == [
        'waffles.food/product/express-shipping',
        'waffles.food/product/extra-waffles',
        'waffles.food/product/waffles',
    ]

    # re-adding should result in errors
    for product in input_product_data:
        with pytest.raises(ValueError):
            add_store_product(dynamodb, **product)


@moto.mock_dynamodb2
def test_update_store_product_tags(
    create_dynamodb_tables, input_product_data, expected_dynamodb_data
):
    dynamodb = boto3.resource('dynamodb')
    create_dynamodb_tables()

    for product in input_product_data:
        add_store_product(dynamodb, **product)

    product_urls = [item['product_url'] for item in input_product_data]

    for tag in [ProductTag.image_not_indexed, ProductTag.update_product_meta]:
        delete_product_tags(dynamodb, tag, product_urls)
        # verify tags were deleted
        assert list(fetch_product_tags(dynamodb, product_urls, tag)) == []

    # overwrite existing product with a different primary image URLs
    update_store_product(
        dynamodb,
        product_url='https://waffles.food/product/waffles',
        store_domain='waffles.food',
        vendor_name='waffle co',
        store_product_brand_domain='waffles.food',
        image_urls=[
            'https://waffles.food/images/new-waffles',
            'https://waffles.food/images/new-waffles-on-plate',
        ],
    )

    # overwrite another existing product with a different "store product brand domain"
    # (waffles.be)
    update_store_product(
        dynamodb,
        product_url='https://waffles.food/product/extra-waffles',
        store_domain='waffles.food',
        vendor_name='waffle co',
        store_product_brand_domain='waffles.be',
        image_urls=['https://waffles.food/images/so-many-waffles'],
    )

    product_urls = [item['product_url'] for item in input_product_data]

    # expect only first product to have "image not indexed tag"
    assert [
        (t['store_product_url'], t['image_url'])
        for t in fetch_product_tags(dynamodb, product_urls, ProductTag.image_not_indexed)
    ] == [
        ('waffles.food/product/waffles', 'https://waffles.food/images/new-waffles'),
    ]

    # expect only second product to have "update product meta tag"
    assert [
        t['store_product_url']
        for t in fetch_product_tags(dynamodb, product_urls, ProductTag.update_product_meta)
    ] == [
        'waffles.food/product/extra-waffles',
    ]


@moto.mock_dynamodb2
def test_write_store_product_update(create_dynamodb_tables, input_product_data):
    dynamodb = boto3.resource('dynamodb')
    create_dynamodb_tables()

    # update non-existing products should result in errors
    for product in input_product_data:
        with pytest.raises(ValueError):
            update_store_product(dynamodb, **product)

    for product in input_product_data:
        add_store_product(dynamodb, **product)

    old_item_data = get_store_product(dynamodb, 'https://waffles.food/product/waffles')

    # update existing product (replace vendor name, add product type)
    update_store_product(
        dynamodb,
        # this should map to the same "store product" as the first write
        # (same URL, excepting scheme and query string)
        # (updates "full store product URL" and "image urls")
        product_url='http://waffles.food/product/waffles?waffle=rofl',
        store_domain='waffles.food',
        vendor_name='waffles 4 all',
        product_type='food'
    )

    new_item_data = get_store_product(dynamodb, 'https://waffles.food/product/waffles')

    # validate item attributes defined in "input_product_data" fixture
    assert old_item_data.pop('full_store_product_url') == 'https://waffles.food/product/waffles'
    assert old_item_data.pop('vendor_name') == 'waffle co'

    # validate updated and additional item attributes
    assert new_item_data.pop('full_store_product_url') == \
        'http://waffles.food/product/waffles?waffle=rofl'
    assert new_item_data.pop('product_type') == 'food'
    assert new_item_data.pop('vendor_name') == 'waffles 4 all'

    # validate all other attributes are unchanged
    assert old_item_data == new_item_data


@moto.mock_dynamodb2
def test_fetch_products_by_brand(create_dynamodb_tables, input_product_data):
    dynamodb = boto3.resource('dynamodb')
    create_dynamodb_tables()

    n_products = 20
    brand_domains = ['abrand.com', 'bbrand.com']
    store_domain = 'store.com'

    for i in range(n_products):
        product_url = f'https://{store_domain}/product-{i}'
        brand_domain = brand_domains[i % 2]
        add_store_product(
            dynamodb,
            product_url=f'https://{store_domain}/product-{i}',
            store_domain=store_domain,
            title=f'Product {i}',
            scraper_type='generic_scraper',
            first_scraped_at=parse_dt('2020-06-01T00:00:01+00:00'),
            last_scraped_at=parse_dt('2020-06-01T00:00:01+00:00'),
        )
        update_product_attribute(
            dynamodb, product_url, 'brand_domain', brand_domain
        )

    for brand_domain in brand_domains:
        products = list(fetch_products_by_brand(dynamodb, brand_domain))

        assert len(products) == n_products / 2
        assert {p['brand_domain'] for p in products} == set([brand_domain])


@moto.mock_dynamodb2
def test_fetch_products_by_store(create_dynamodb_tables, input_product_data):
    dynamodb = boto3.resource('dynamodb')
    create_dynamodb_tables()

    n_products = 20
    store_domains = ['astore.com', 'bstore.com']

    for i in range(n_products):
        store_domain = store_domains[i % 2]
        add_store_product(
            dynamodb,
            product_url=f'https://{store_domain}/product-{i}',
            store_domain=store_domain,
            title=f'Product {i}',
            scraper_type='generic_scraper',
            first_scraped_at=parse_dt('2020-06-01T00:00:01+00:00'),
            last_scraped_at=parse_dt('2020-06-01T00:00:01+00:00'),
        )

    for store_domain in store_domains:
        products = list(fetch_products_by_store(dynamodb, store_domain))

        assert len(products) == n_products / 2
        assert {p['store_domain'] for p in products} == set([store_domain])


@moto.mock_dynamodb2
def test_fetch_products_by_product_uuid(create_dynamodb_tables, input_product_data):
    dynamodb = boto3.resource('dynamodb')
    create_dynamodb_tables()

    n_products = 20
    store_domain = 'store.com'
    product_uuids = [uuid.uuid4(), uuid.uuid4()]

    for i in range(n_products):
        product_url = f'https://{store_domain}/product-{i}'
        product_uuid = product_uuids[i % 2]
        add_store_product(
            dynamodb,
            product_url=product_url,
            store_domain=store_domain,
            title=f'Product {i}',
            scraper_type='generic_scraper',
            first_scraped_at=parse_dt('2020-06-01T00:00:01+00:00'),
            last_scraped_at=parse_dt('2020-06-01T00:00:01+00:00'),
        )
        update_product_attribute(
            dynamodb, product_url, 'product_uuid', product_uuid.hex
        )

    for product_uuid in product_uuids:
        products = list(fetch_products_by_product_uuid(dynamodb, product_uuid.hex))

        assert len(products) == n_products / 2
        assert {p['product_uuid'] for p in products} == set([product_uuid.hex])


@moto.mock_dynamodb2
def test_delete_store_products(
    create_dynamodb_tables, input_product_data, expected_dynamodb_data
):
    dynamodb = boto3.resource('dynamodb')
    create_dynamodb_tables()
    to_delete = [
        'waffles.food/product/extra-waffles',
        'waffles.food/product/waffles',
    ]

    for product in input_product_data:
        add_store_product(dynamodb, **product)

    for store_product_url in to_delete:
        assert get_store_product(dynamodb, store_product_url) is not None
        assert len(list(fetch_product_tags(dynamodb, [store_product_url]))) > 0

    delete_store_products(dynamodb, to_delete)

    for store_product_url in to_delete:
        assert get_store_product(dynamodb, store_product_url) is None
        assert len(list(fetch_product_tags(dynamodb, [store_product_url]))) == 0
