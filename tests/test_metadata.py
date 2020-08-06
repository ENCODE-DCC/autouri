from binascii import hexlify

from autouri.metadata import base64_to_hex, get_seconds_from_epoch, parse_md5_str


def test_get_seconds_from_epoch():
    assert get_seconds_from_epoch("Sat, 07 Mar 2020 21:03:07 GMT") == 1583614987.0
    assert get_seconds_from_epoch("Sat Mar  7 15:05:07 PST 2020") == 1583622307.0
    assert get_seconds_from_epoch("2017-07-01T14:59:55.711Z") == 1498921195.711
    assert get_seconds_from_epoch("2017-03-10 14:30:12,655+0000") == 1489156212.655


def test_base64_to_hex():
    assert base64_to_hex("aGVsbG8gd29ybGQ=") == hexlify("hello world".encode()).decode()
    assert (
        base64_to_hex("YXNkZnNkZnNhZGYjQEtKV0xLSkFTRDw+S0ZKQCNBU0RDQ0FTREBAIyRSIQ==")
        == hexlify("asdfsdfsadf#@KJWLKJASD<>KFJ@#ASDCCASD@@#$R!".encode()).decode()
    )
    assert (
        base64_to_hex("NWEyNjUzYTY2Zjk1OGY5ZmNiODYwMGUyMDI4MTFiMjc=")
        == hexlify("5a2653a66f958f9fcb8600e202811b27".encode()).decode()
    )


def test_parse_md5_str():
    assert (
        parse_md5_str("WiZTpm+Vj5/LhgDiAoEbJw==") == "5a2653a66f958f9fcb8600e202811b27"
    )
    assert (
        parse_md5_str("5a2653a66f958f9fcb8600e202811b27")
        == "5a2653a66f958f9fcb8600e202811b27"
    )
