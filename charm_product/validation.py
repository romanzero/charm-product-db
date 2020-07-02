import re
import decimal
import json
import pytz
import warnings
from boto3.dynamodb.types import TypeSerializer
from collections import namedtuple
from urllib.parse import urlsplit


dynamo_type_serializer = TypeSerializer()


MINIMUM_PRICE = decimal.Decimal('0.02')


class ValidationError(Exception):
    pass


class ValidationWarning(UserWarning):
    pass


def format_token_group_regexes(token_groups):
    """
    Convert space-delimited token groups into compiled regexes for matching
    groups of sequential tokens
    """
    return [
        re.compile(f'(^| ){tkn_grp}($| )')
        for tkn_grp in token_groups
    ]


IMAGE_URL_BLACKLIST_TOKEN_GROUPS = format_token_group_regexes([
    'noimage',
    'no image',
    'nophoto',
    'no photo',
    'placeholder',
])

PRODUCT_TITLE_BLACKLIST_TOKEN_GROUPS = format_token_group_regexes([
    'test product',
    'gift card',
    'egift card',
    'digital card',
    'virtual card',
    'gift voucher',
    'custom gift box',
    'promo card',
    'promotion card',
    'tarjeta regalo',
    'insurance',
    'upgrade shipping',
    'shipping upgrade',
    'service fee',
    'handling fee',
    'credit card fee',
    'credit card surcharge',
    'in store pickup',
    'instore pickup',
    'pickup in store',
    'pickup instore',
    'item customizations',
    'item personalization',
    'bottle deposit',
])


TOKENIZER_REGEX = re.compile(r'[\W_]+')


def contains_blacklist_tokens(text, token_groups):
    # Split on non-word characters and put a single space between
    # all tokens (allow matching on groups of consecutive tokens)
    tokenized_text = ' '.join(TOKENIZER_REGEX.split(text)).lower()

    for token_group in token_groups:
        match = re.search(token_group, tokenized_text)
        if match is not None:
            return True, match.group(0).strip()
    return False, None


def product_title(title):
    if not title:
        raise ValidationError('Product must have a title')

    invalid, token_match = contains_blacklist_tokens(
        title, PRODUCT_TITLE_BLACKLIST_TOKEN_GROUPS
    )
    if invalid:
        raise ValidationError(
            f'Blacklisted token(s) "{token_match}" in product title'
        )
    return title


def image_url(url):
    url_parts = urlsplit(url)

    if not url_parts.netloc:
        raise ValidationError('Invalid URL (missing netloc)')
    if url_parts.scheme not in ['http', 'https']:
        raise ValidationError('Invalid URL (scheme != "http/https")')

    invalid, token_match = contains_blacklist_tokens(
        url_parts.path, IMAGE_URL_BLACKLIST_TOKEN_GROUPS
    )
    if invalid:
        raise ValidationError(
            f'Blacklisted token(s) "{token_match}" in URL path'
        )
    return url


def price(price):
    try:
        price = decimal.Decimal(price)
        if not price.is_finite():
            raise ValidationError('NaN/infinite decimal value not allowed')
        return price
    except decimal.InvalidOperation:
        raise ValidationError(f'Invalid price value: {price}')


def iso_date_string(dt):
    try:
        return pytz.utc.localize(dt).isoformat()
    except ValueError:
        return dt.astimezone(pytz.utc).isoformat()
    except AttributeError:
        raise ValidationError(f'Invalid datetime value: {dt}')


def string(x):
    if not isinstance(x, str):
        raise ValidationError(f'Value is not a string: {x}')
    return x


def json_string(x):
    json.loads(x)
    return x


def string_list(x):
    try:
        serialized = dynamo_type_serializer.serialize(x)
        if (
            set(serialized.keys()) != {'L'} or
            {
                elem_type
                for elem in list(serialized.values())[0]
                for elem_type in elem.keys()
            } != {'S'}
        ):
            raise ValidationError(f'Invalid "string list" value: {x}')
    except TypeError:
        raise ValidationError(f'Invalid "string list" value: {x}')

    return x


AttributeSchema = namedtuple(
    'AttributeSchema',
    ['name', 'type', 'required'],
)
AttributeSchema.__new__.__defaults__ = (
    None, None, False
)

ITEM_ATTRIBUTES = [
    AttributeSchema('store_product_url', string, required=True),
    AttributeSchema('full_store_product_url', string, required=True),
    AttributeSchema('store_domain', string, required=True),
    AttributeSchema('is_available', int, required=True),
    AttributeSchema('title', product_title, required=True),
    AttributeSchema('description', string),
    AttributeSchema('image_urls', string_list),
    AttributeSchema('product_type', string),
    AttributeSchema('published_at', iso_date_string),
    AttributeSchema('created_at', iso_date_string),
    AttributeSchema('updated_at', iso_date_string),
    AttributeSchema('removed_at', iso_date_string),
    AttributeSchema('primary_currency', string),
    AttributeSchema('primary_price', price),
    AttributeSchema('best_selling_position', int),
    AttributeSchema('vendor_name', string),
    AttributeSchema('store_product_brand_domain', string),
    AttributeSchema('store_product_brand_domain_association', string),
    AttributeSchema('store_platform', string),
    AttributeSchema('first_scraped_at', iso_date_string, required=True),
    AttributeSchema('last_scraped_at', iso_date_string, required=True),
    AttributeSchema('scraper_type', string, required=True),
    AttributeSchema('json_data', json_string),
]

VALID_ATTRIBUTES = set(attr.name for attr in ITEM_ATTRIBUTES)

REQUIRED_ATTRIBUTES = set(attr.name for attr in ITEM_ATTRIBUTES if attr.required)


def parse_store_product_data(
    item_data,
    new_item=True,
    warn_invalid_attributes=False,
):
    invalid_attrs = [k for k in item_data if k not in VALID_ATTRIBUTES]
    if invalid_attrs:
        raise ValidationError(f'Invalid attributes: {invalid_attrs}')

    parsed_data = {}
    for attr in ITEM_ATTRIBUTES:
        if attr.name in item_data:
            try:
                parsed_data[attr.name] = attr.type(item_data[attr.name])
            except (TypeError, ValueError, ValidationError) as err:
                if warn_invalid_attributes:
                    warnings.warn(
                        f'Skipping "{attr.name}" attribute with invalid data '
                        f'({item_data[attr.name]})',
                        ValidationWarning,
                    )
                elif isinstance(err, ValidationError):
                    raise
                else:
                    raise ValidationError(err)

            if attr.name == 'image_urls':
                valid_image_urls = []
                for url in parsed_data['image_urls']:
                    try:
                        valid_image_urls.append(image_url(url))
                    except ValidationError:
                        if warn_invalid_attributes:
                            warnings.warn(
                                f'Skipping invalid image URL: {url}',
                                ValidationWarning,
                            )
                        else:
                            raise
                if valid_image_urls:
                    parsed_data['image_urls'] = valid_image_urls
                else:
                    del parsed_data['image_urls']

    item_data = parsed_data

    if 'primary_price' in item_data and item_data['primary_price'] < MINIMUM_PRICE:
        raise ValidationError(f'Primary price value must be >= {MINIMUM_PRICE}')

    if new_item:
        missing_attrs = [k for k in (REQUIRED_ATTRIBUTES - set(item_data.keys()))]
        if missing_attrs:
            raise ValidationError(f'Missing required attributes: {missing_attrs}')

    return item_data
