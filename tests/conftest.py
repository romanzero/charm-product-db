import os
import pytest
from mock import patch

from charm_product.schema.migrate_0001_create_product_tables import migrate as migrate1
from charm_product.schema.migrate_0002_replace_product_tag_image_index import migrate as migrate2
from charm_product.schema.migrate_0003_replace_product_tag_meta_index import migrate as migrate3
from charm_product.schema.migrate_0004_delete_visual_features_table import migrate as migrate4


@pytest.fixture(scope='session', autouse=True)
def mock_env():
    """mock aws credentials to allow mock AWS testing with moto"""
    with patch.dict(os.environ, {
        'CHARM_PRODUCT_ENV': 'dev',
        'AWS_DEFAULT_REGION': 'us-west-2',
        'AWS_ACCESS_KEY_ID': 'AKIA0000000000000000',
        'AWS_SECRET_ACCESS_KEY': '0000000000000000000000000000000000000000,'
    }):
        yield


@pytest.fixture()
def create_dynamodb_tables():

    def func():
        migrate1()
        migrate2()
        migrate3()
        migrate4()

    return func
