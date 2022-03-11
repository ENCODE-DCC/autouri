"""Make sure to run these tests on a machine with correct os time
"""
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from autouri.ntp_now import get_cached_offset, now_utc, reset_cached_offset


def test_now_utc():
    reset_cached_offset()

    assert abs((now_utc() - datetime.now(timezone.utc)).total_seconds()) < 0.01


class MockedDataTime(datetime):
    @classmethod
    def now(cls, timezone):
        return datetime.now(timezone) - timedelta(0, 25)


def test_now_utc_wrong_os_time():
    correct_now = datetime.now(timezone.utc)

    with patch("autouri.ntp_now.datetime", MockedDataTime):
        reset_cached_offset()

        # should be accurate even though system time is 25 second behind NTP server time
        assert abs((now_utc() - correct_now).total_seconds()) < 0.01

        # cache offset should be 25 second
        cached_offset_in_seconds = get_cached_offset().total_seconds()
        assert cached_offset_in_seconds > 24.99 and cached_offset_in_seconds < 25.01
