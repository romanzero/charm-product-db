import json
import pytest
import pytz
import warnings
from datetime import datetime
from decimal import Decimal

from charm_product.validation import (
    MINIMUM_PRICE, REQUIRED_ATTRIBUTES, ValidationError, ValidationWarning,
    parse_store_product_data
)


@pytest.fixture()
def valid_store_product():
    return dict(
        store_product_url='xyz.com/products/best-product',
        full_store_product_url='https://xyz.com/products/best-product',
        store_domain='xyz.com',
        is_available=True,
        title='Best Product Ever',
        description='This is the best product',
        image_urls=['https://xyz.com/best-product.jpg'],
        product_type='product',
        published_at=datetime(2020, 6, 1),
        created_at=datetime(2020, 6, 1),
        updated_at=datetime(2020, 6, 1),
        removed_at=datetime(2020, 6, 1),
        primary_currency='USD',
        primary_price='30.00',
        best_selling_position=1,
        vendor_name='XYZ Product Co.',
        store_product_brand_domain='xyz.com',
        store_product_brand_domain_association='vendorname2brand',
        store_platform='shopify',
        first_scraped_at=datetime(2020, 6, 1),
        last_scraped_at=pytz.utc.localize(datetime(2020, 6, 1)),
        scraper_type='shopify_scraper',
        json_data=json.dumps({'tokens': ['xyz', 'best', 'product']}),
    )


def test_parse_store_product_data_valid(valid_store_product):
    parse_store_product_data(valid_store_product)


@pytest.mark.parametrize(['invalid_attr_name', 'invalid_attr_value'], [
    ('store_product_url', 123),
    ('full_store_product_url', 123),
    ('store_domain', 123),
    ('is_available', 'yes'),
    ('title', 'Gift Card'),
    ('description', 123),
    ('image_urls', ['https://xyz.com/placeholder.jpg', 'image.jpg']),
    ('product_type', 123),
    ('published_at', '2020-06-01'),
    ('created_at', 123),
    ('updated_at', None),
    ('removed_at', True),
    ('primary_currency', 123),
    ('primary_price', '$30.00'),
    ('best_selling_position', 'one'),
    ('vendor_name', 123),
    ('store_product_brand_domain', 123),
    ('store_product_brand_domain_association', 123),
    ('store_platform', 123),
    ('first_scraped_at', None),
    ('last_scraped_at', 'not a date'),
    ('scraper_type', 123),
    ('json_data', 'xyz,best,product'),
])
def test_parse_store_product_data_invalid(
    valid_store_product, invalid_attr_name, invalid_attr_value
):
    invalid_store_product = valid_store_product.copy()
    invalid_store_product[invalid_attr_name] = invalid_attr_value

    with pytest.raises(ValidationError):
        parse_store_product_data(invalid_store_product)

    with warnings.catch_warnings():
        # do not show warnings
        warnings.filterwarnings('ignore', category=ValidationWarning)

        if invalid_attr_name in REQUIRED_ATTRIBUTES:
            # Adding a "new item" input with invalid "required" attributes
            # should raise a ValidationError, regardless of whether
            # "warn_invalid_attributes" is True
            with pytest.raises(ValidationError):
                parse_store_product_data(
                    invalid_store_product,
                    new_item=True, warn_invalid_attributes=True
                )
            # If updating an existing item, dropping invalid "required"
            # attributes is acceptable
            item_data = parse_store_product_data(
                invalid_store_product,
                new_item=False, warn_invalid_attributes=True
            )
            assert invalid_attr_name not in item_data
        else:
            # Expect invalid attributes to be dropped without raising errors
            item_data = parse_store_product_data(
                invalid_store_product,
                new_item=True, warn_invalid_attributes=True
            )
            assert invalid_attr_name not in item_data


@pytest.mark.parametrize(['invalid_price'], [
    (MINIMUM_PRICE - Decimal('0.001'),),
    ('0',),
    (-1,),
])
def test_parse_store_product_data_invalid_price(
    valid_store_product, invalid_price
):
    invalid_store_product = valid_store_product.copy()
    invalid_store_product['primary_price'] = invalid_price

    # Never allow adding products priced below the minimum price threshold
    with pytest.raises(ValidationError):
        parse_store_product_data(invalid_store_product)
        parse_store_product_data(
            invalid_store_product, new_item=False,
        )
        parse_store_product_data(
            invalid_store_product, warn_invalid_attributes=True
        )
