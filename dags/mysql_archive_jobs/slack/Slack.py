import os
from slack_sdk import WebClient
import logging


logger = logging.getLogger(__name__)


class RobotAnnouncement:

    @staticmethod
    def PostToChannelProxy(token: str, proxy: str, channel_id: str, message: str) -> bool:

        client = None
        
        if proxy or proxy is not None:
            os.environ["http_proxy"] = proxy
            os.environ["https_proxy"] = proxy
            client = WebClient(token=token, proxy=proxy)
            
        else:
            client = WebClient(token=token)   

        try:
            response = client.chat_postMessage(
                channel=channel_id,
                mrkdwn=True,
                text=message,
            )
            return response.get("ok", False)
        except Exception as e:
            logging.exception("An exception is occurred while send message through the Slack API")
            return False
