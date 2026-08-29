import os

from config import appsetting
from slack.Slack import RobotAnnouncement

_proxy = None


def init_slack(proxy) -> None:
    global _proxy
    _proxy = proxy


def send_msg_to_multiple_slack_channel(msg) -> None:
    RobotAnnouncement.PostToChannelProxy(
        appsetting.SLACK_TOKEN,
        _proxy,
        appsetting.SLACK_CHANNEL,
        msg,
    )
    os.environ.pop("http_proxy", None)
    os.environ.pop("https_proxy", None)
