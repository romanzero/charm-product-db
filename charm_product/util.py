import os
from urllib.parse import urlsplit


VALID_ENVS = [
    'dev',
    'staging',
    'prod',
]

# Product UUIDs for invalid products. Add UUIDs for products that should
# not be retrieved or used in any feature calculations here.
PRODUCT_UUID_BLACKLIST = set([
    'fee0e2fe426e4da7aaf8581772579dd8',  # Products using Shopify default "gift card" image
])


def get_env_prefix():
    env = os.environ.get('CHARM_PRODUCT_ENV')

    if env not in VALID_ENVS:
        raise RuntimeError(
            'Environment variable for "CHARM_PRODUCT_ENV" is not set or is invalid '
            '(should be one of {})'
            .format(', '.join([f'"{e}"' for e in VALID_ENVS]))
        )
    return f'charm_{env}'.lower()


def get_table_name(name):
    env_prefix = get_env_prefix()
    return f'{env_prefix}_{name}'


def clean_product_url(product_url):
    """
    Get a product key from a URL

    Strips URL scheme, query string, etc. leaving only the domain and path
    (ensure URLS with different query strings or HTTP/HTTPS do not resolve
    to different DynamoDB keys)
    """
    parts = urlsplit(product_url)

    path = parts.path
    if path and path.endswith('/'):
        path = path[:-1]

    if parts.hostname:
        clean_url = f'{parts.hostname}{path}'
    else:
        # URL does not include scheme prefix (*//)
        # URL split will not parse "hostname" separately from path
        clean_url =  path

    if clean_url.startswith('www.'):
        clean_url = clean_url[4:]
    return clean_url
