# -*- coding: utf-8 -*-
import mock
from test_util import StubRedis
from wayback_discover_diff.discover import (extract_html_features,
    calculate_simhash, custom_hash_function, pack_simhash_to_bytes, Discover)


def test_extract_html_features():
    # handle html with repeated elements and spaces
    html = """<html>
<title>my title</title>
<body>
abc
test

123
abc
  space
</body>
</html>"""
    features = {'123': 1, 'abc': 2, 'my': 1, 'test': 1, 'title': 1, 'space': 1}
    assert extract_html_features(html) == features

    # handle html with repeated elements, and punctuation
    html = """<html>
<title>my title</title>
<body>
abc
a.b.c.
abc.
test
123
abc
</body>
</html>"""
    features = {'123': 1, 'a': 1, 'abc': 3, 'b': 1, 'c': 1, 'my': 1, 'test': 1, 'title': 1}
    assert extract_html_features(html) == features

    # handle plain text
    html = "just a string"
    features = {'just': 1, 'a': 1, 'string': 1}
    assert extract_html_features(html) == features

    # skip HTML comments
    html = """<html><head>
</head><body>
<!--[if lt IE 9]>
<!-- Important Owl stylesheet -->
<link rel="stylesheet" href="css/owl.carousel.css">
<!-- Default Theme -->
<link rel="stylesheet" href="css/owl.theme.css">
<script src="js/html5shiv.js"></script>
<script src="js/respond.min.js"></script>
<![endif]-->
<p>Thank you for closing the message box.</p>
<a href="/subpage">test</a>
</body></html>"""
    features = {'box': 1, 'closing': 1, 'for': 1, 'message': 1, 'test': 1,
                'thank': 1, 'the': 1, 'you': 1}
    assert extract_html_features(html) == features

    # it doesn't crash with invalid or unicode chars
    html = """<html>
<title>Invalid /\x94Invalid\x0b'</title>
<body>
今日は

</body>
</html>"""
    features = {'\x94invalid': 1, 'invalid': 1, '今日は': 1}
    assert extract_html_features(html) == features


    html = """<Html>
    <something>weird is happening \c\x0b
    <span>tag</span><span>tag</span>
    </HTML>"""

    features = {'c': 1, 'weird': 1, 'is': 1, 'happening': 1, 'tag': 2}
    assert extract_html_features(html) == features


def test_calculate_simhash():
    features = {'two': 2, 'three': 3, 'one': 1}
    assert calculate_simhash(features, 128) == 66237222457941138286276456718971054176


CFG = {
    'simhash': {
        'size': 256,
        'expire_after': 86400
        },
    'redis': {
        'url': 'redis://localhost:6379/1',
        'decode_responses': True,
        'timeout': 10
        },
    'threads': 5,
    'snapshots': {
        'number_per_year': -1,
        'number_per_page': 600
        }
    }

@mock.patch('wayback_discover_diff.discover.StrictRedis')
def test_worker_download(Redis):
    Redis.return_value = StubRedis()
    task = Discover(CFG)
    # This capture performs redirects inside WBM. It has CDX status=200 but
    # its really a redirect (This is a common WBM issue). We test that
    # redirects work fine.
    task.url = 'http://era.artiste.universalmusic.fr/'
    assert task.download_capture('20180716140623')


def test_regular_hash():
    features = {
        '2019': 1,
        'advanced': 1,
        'google': 1,
        'google©': 1,
        'history': 1,
        'insearch': 1,
        'more': 1,
        'optionssign': 1,
        'privacy': 1,
        'programsbusiness': 1,
        'searchimagesmapsplayyoutubenewsgmaildrivemorecalendartranslatemobilebooksshoppingbloggerfinancephotosvideosdocseven': 1,
        'searchlanguage': 1,
        'settingsweb': 1,
        'solutionsabout': 1,
        'terms': 1,
        'toolsadvertising': 1,
        '»account': 1
    }
    h = calculate_simhash(features, 128)
    assert h.bit_length() == 128
    h_bytes = pack_simhash_to_bytes(h)
    assert len(h_bytes) == 16


def test_shortened_hash():
    h_size = 128
    features = {
        'about': 1,
        'accountsearchmapsyoutubeplaynewsgmailcontactsdrivecalendartranslatephotosshoppingmorefinancedocsbooksbloggerhangoutskeepjamboardearthcollectionseven': 1,
        'at': 1,
        'data': 1,
        'feedbackadvertisingbusiness': 1,
        'from': 1,
        'gmailimagessign': 1,
        'google': 3,
        'helpsend': 1,
        'in': 2,
        'inappropriate': 1,
        'library': 1,
        'local': 1,
        'more': 1,
        'new': 1,
        'predictions': 1,
        'privacytermssettingssearch': 1,
        'remove': 1,
        'report': 1,
        'searchhistorysearch': 1,
        'searchyour': 1,
        'settingsadvanced': 1,
        'skills': 1,
        'store': 1,
        'with': 1,
        'your': 1,
        '×develop': 1
    }
    h = calculate_simhash(features, h_size)
    assert h.bit_length() != h_size
    h_bytes = pack_simhash_to_bytes(h, h_size)
    assert len(h_bytes) == h_size // 8


def test_simhash_256():
    h_size = 256
    features = {
        '2019': 1,
        'advanced': 1,
        'at': 1,
        'google': 1,
        'googleadvertising': 1,
        'google©': 1,
        'history': 1,
        'insearch': 1,
        'library': 1,
        'local': 1,
        'more': 1,
        'new': 1,
        'optionssign': 1,
        'privacy': 1,
        'programsbusiness': 1,
        'searchimagesmapsplayyoutubenewsgmaildrivemorecalendartranslatemobilebooksshoppingbloggerfinancephotosvideosdocseven': 1,
        'searchlanguage': 1,
        'settingsweb': 1,
        'skills': 1,
        'solutionsabout': 1,
        'terms': 1,
        'toolsdevelop': 1,
        'with': 1,
        'your': 1,
        '»account': 1,
    }
    h = calculate_simhash(features, h_size, custom_hash_function)
    assert h.bit_length() == h_size
    h_bytes = pack_simhash_to_bytes(h, h_size)
    assert len(h_bytes) == h_size // 8
