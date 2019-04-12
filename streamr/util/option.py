"""
This doc provide option class
"""

from streamr.util.constant import Addrs, OptionConstant, AuthConstant
from streamr.util.compare import EqualFunc

__all__ = ['Option', 'Auth']


class Option(EqualFunc):
    """
    Client options
    """

    @classmethod
    def get_default_option(cls):
        """
        return the default Option object
        :return: Option
        """
        return Option(url=Addrs.WS_ADDR, rest_url=Addrs.REST_ADDR,
                      auto_connect=True, auto_disconnect=True)

    def __init__(self, api_key=None, url=None, rest_url=None,
                 auto_connect=None, auto_disconnect=None,
                 session_token_refresh_interval=7200, auth=None,
                 auth_key=None, stream_id=None, stream_partition=None,
                 resend_all=None, resend_from=None, resend_to=None,
                 resend_last=None, resend_from_time=None):

        self.api_key = api_key
        self.url = url
        self.rest_url = rest_url
        self.auto_connect = auto_connect
        self.auto_disconnect = auto_disconnect
        self.session_token_refresh_interval = session_token_refresh_interval
        self.auth = auth if isinstance(auth, Auth) else Auth()
        self.auth_key = auth_key
        self.stream_id = stream_id
        self.stream_partition = stream_partition
        self.resend_all = resend_all
        self.resend_from = resend_from
        self.resend_to = resend_to
        self.resend_last = resend_last
        self.resend_from_time = resend_from_time

    def to_object(self):
        """
        convert Option to dict
        :return: dict
        """
        dic = {OptionConstant.API_KEY: self.api_key,
               OptionConstant.URL: self.url,
               OptionConstant.REST_URL: self.rest_url,
               OptionConstant.AUTO_CONNECT: self.auto_connect,
               OptionConstant.AUTO_DISCONNECT: self.auto_disconnect,
               OptionConstant.SESSION_REFRESH_INTERVAL: self.session_token_refresh_interval,
               OptionConstant.AUTH: self.auth.to_object(),
               OptionConstant.AUTH_KEY: self.auth_key,
               OptionConstant.STREAM_ID: self.stream_id,
               OptionConstant.STREAM_PARTITION: self.stream_partition,
               OptionConstant.RESEND_ALL: self.resend_all,
               OptionConstant.RESEND_FROM: self.resend_from,
               OptionConstant.RESEND_TO: self.resend_to,
               OptionConstant.RESEND_LAST: self.resend_last,
               OptionConstant.RESEND_FROM_TIME: self.resend_from_time}
        for k, v in dic:
            if v is None:
                dic.pop(k)
        return dic

    def to_resend_object(self):
        """
        convert Option to dict
        :return: dict
        """
        dic = {OptionConstant.RESEND_ALL: self.resend_all,
               OptionConstant.RESEND_FROM: self.resend_from,
               OptionConstant.RESEND_TO: self.resend_to,
               OptionConstant.RESEND_LAST: self.resend_last,
               OptionConstant.RESEND_FROM_TIME: self.resend_from_time}
        for k in list(dic.keys()):
            if dic[k] is None:
                dic.pop(k)
        return dic

    @classmethod
    def deserialize(cls, msg):
        """
        deserialize dict to options
        :param msg: dict
        :return: Option
        """
        if not isinstance(msg, dict):
            raise ValueError('To deserialize Option, only dict object is supported. Given : %s ' % type(msg))

        args = [msg.get(OptionConstant.API_KEY),
                msg.get(OptionConstant.URL),
                msg.get(OptionConstant.REST_URL),
                msg.get(OptionConstant.AUTO_CONNECT),
                msg.get(OptionConstant.AUTO_DISCONNECT),
                msg.get(OptionConstant.SESSION_REFRESH_INTERVAL, 7200),
                Auth.deserialize(msg.get(OptionConstant.AUTH)),
                msg.get(OptionConstant.AUTH_KEY),
                msg.get(OptionConstant.STREAM_ID),
                msg.get(OptionConstant.STREAM_PARTITION),
                msg.get(OptionConstant.RESEND_ALL),
                msg.get(OptionConstant.RESEND_FROM),
                msg.get(OptionConstant.RESEND_TO),
                msg.get(OptionConstant.RESEND_LAST),
                msg.get(OptionConstant.RESEND_FROM_TIME)]
        return Option(*args)

    @classmethod
    def deserialize_resend(cls, msg):
        """
        deserialize dict to resend options
        :param msg: dict
        :return: Option
        """
        if not isinstance(msg, dict):
            raise ValueError('To deserialize Option, only dict object is supported. Given : %s ' % type(msg))

        return Option(resend_all=msg.get(OptionConstant.RESEND_ALL),
                      resend_from=msg.get(OptionConstant.RESEND_FROM),
                      resend_to=msg.get(OptionConstant.RESEND_TO),
                      resend_from_time=msg.get(OptionConstant.RESEND_FROM_TIME),
                      resend_last=msg.get(OptionConstant.RESEND_LAST))

    def check_api(self):
        """
        check whether Client option has an apiKey
        :return: bool
        """
        if self.api_key is None and self.auth_key is None and self.auth.api_key is None:
            raise ValueError('Option should contain the apiKey')
        else:
            if self.api_key is None and self.auth_key is not None:
                self.api_key = self.auth_key
            if self.api_key is not None:
                self.auth.api_key = self.api_key
            return True

    def check_url(self):
        """
        check whether Option has url and rest_url
        :return: bool
        """
        if not isinstance(self.url, str) or ':' not in self.url:
            raise ValueError('url should be given.'
                             ' You can use Client.get_default_option() '
                             'to create a default ClientOption')
        if not isinstance(self.rest_url, str) or ':' not in self.rest_url:
            raise ValueError('rest url should ge given. '
                             'You can use Client.get_default_option() '
                             'to create a default ClientOption')

    def check_resend(self):
        """
        return the number of resend options in Option object
        :return:int
        """
        count = 0
        if self.resend_all is not None:
            count += 1
        if self.resend_from is not None:
            count += 1
        if self.resend_last is not None:
            count += 1
        if self.resend_from_time is not None:
            count += 1
        return count


class Auth(EqualFunc):
    """
    Auth object
    """

    def __init__(self, api_key=None, private_key=None,
                 provider=None, user_name=None,
                 password=None, session_token=None):
        self.api_key = api_key
        self.private_key = private_key
        self.provider = provider
        self.user_name = user_name
        self.password = password
        self.session_token = session_token

    def to_object(self):
        """
        convert Auth to dict
        :return: dict
        """
        dic = {AuthConstant.API_KEY: self.api_key,
               AuthConstant.PRIVATE_KEY: self.private_key,
               AuthConstant.PROVIDER: self.provider,
               AuthConstant.USER_NAME: self.user_name,
               AuthConstant.PASSWORD: self.password,
               AuthConstant.SESSION_TOKEN: self.session_token}
        for k, v in dic:
            if v is None:
                dic.pop(k)
        return dic

    @classmethod
    def deserialize(cls, msg):
        """
        deserialize auth dict to object
        :param msg: dict
        :return: Auth
        """
        if not isinstance(msg, dict):
            return cls()
        else:
            args = [msg.get(AuthConstant.API_KEY),
                    msg.get(AuthConstant.PRIVATE_KEY),
                    msg.get(AuthConstant.PROVIDER),
                    msg.get(AuthConstant.USER_NAME),
                    msg.get(AuthConstant.PASSWORD),
                    msg.get(AuthConstant.SESSION_TOKEN)]
            return cls(args)
