from charm_product.util import clean_product_url


def test_clean_product_url():
    cleaned_url = 'store.com/products/fork'

    assert clean_product_url(cleaned_url) == cleaned_url
    assert clean_product_url(f'www.{cleaned_url}') == cleaned_url
    assert clean_product_url(f'http://{cleaned_url}') == cleaned_url
    assert clean_product_url(f'https://www.{cleaned_url}') == cleaned_url
    assert clean_product_url(f'//www.{cleaned_url}') == cleaned_url
    assert clean_product_url(f'http://{cleaned_url}?arg=1') == cleaned_url
    assert clean_product_url(f'http://{cleaned_url}#fragment') == cleaned_url

    assert clean_product_url(f'xyz.{cleaned_url}') != cleaned_url
