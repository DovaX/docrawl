import pytest

from docrawl.utils import build_abs_url


def test_build_abs_url():
    # Domain-relative link
    assert build_abs_url('/test', 'https://example.com') == 'https://example.com/test'
    assert build_abs_url('/aaa/bbb/ccc/ddd', 'https://example.com') == 'https://example.com/aaa/bbb/ccc/ddd'
    assert build_abs_url('/ddd/www', 'https://example.com/aaa/bbb/ccc') == 'https://example.com/ddd/www'
    assert build_abs_url('/www/ddd', 'https://example.com/aaa/') == 'https://example.com/www/ddd'

    # Path-relative link
    assert build_abs_url('https://aaaa.com/test', 'https://bbb.com') == 'https://aaaa.com/test'
    assert build_abs_url('test', 'https://example.com') == 'https://example.com/test'
    assert build_abs_url('https://example.com/test', None) == 'https://example.com/test'
    assert build_abs_url('aaa/../bbb/ccc/./ddd', 'https://example.com') == 'https://example.com/bbb/ccc/ddd'
    assert build_abs_url('../ddd/www', 'https://example.com/aaa/bbb/ccc') == 'https://example.com/aaa/bbb/ddd/www'
    assert build_abs_url('../www/../ddd', 'https://example.com/aaa/bbb/ccc') == 'https://example.com/aaa/bbb/ddd'

    # Absolute link
    assert build_abs_url('https://example.com/test') == 'https://example.com/test'
    assert build_abs_url('https://example.com/aaa/bbb/ccc/ddd') == 'https://example.com/aaa/bbb/ccc/ddd'
    with pytest.raises(ValueError):
        build_abs_url('/test/api/d1')
    with pytest.raises(ValueError):
        build_abs_url('test/qwaf/werq')
    with pytest.raises(ValueError):
        build_abs_url('../../..', 'https://example.com/aaa/bbb')
