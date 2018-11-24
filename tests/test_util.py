import pytest
from wayback_discover_diff.util import url_is_valid


@pytest.mark.parametrize("url,result", [
    ('http://example.com/', True),
    ('other', False)
    ])
def test_url_is_valid(url, result):
    assert url_is_valid(url) == result
