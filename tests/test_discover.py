from wayback_discover_diff.discover import (extract_html_features,
    calculate_simhash)


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


def test_calculate_simhash():
    features = {'two': 2, 'three': 3, 'one': 1}
    assert calculate_simhash(features, 128) == 1194485672667776276
