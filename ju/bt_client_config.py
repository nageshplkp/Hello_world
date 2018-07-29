import logging
from abc import ABCMeta, abstractmethod
from enum import Enum
from werkzeug.utils import cached_property
from etl.repo.fnd_cfdw.etl_app_config import EtlAppConfigRepo


class Code(Enum):
    PROGRAMFLAG = 'PROGRAMFLAG'
    PROGRAMNAME = 'PROGRAMNAME'
    USERNUMBER = 'USERNUMBER'
    FIRMNAME = 'FIRMNAME'
    BT_ENDPOINT = 'BT_ENDPOINT'
    BT_REQ_CODE = 'BT_REQ_CODE'
    LOG_LEVEL = 'LOG_LEVEL'
    BT_FORMAT = 'BT_FORMAT'
    BT_PROGRAM_CODE = 'BT_PROGRAM_CODE'
    INTERFACE_CODE = 'INTERFACE_CODE'
    REQUEST_LIMIT = 'REQUEST_LIMIT'
    POLL_LIMIT = 'POLL_LIMIT'
    UNKNOWN = '#UNK#'

    @staticmethod
    def get(key):
        try:
            return Code[key]
        except KeyError:
            return Code.UNKNOWN


# Configuration Base
class BtClientConfigBase:

    NO_ITEMS_PROCESSED = 100
    ITEMS_PROCESSED = 0
    """
    This is a Configuration Base Class for BT client.
    """
    __metaclass__ = ABCMeta
    CODES = Code

    @abstractmethod
    def __init__(self, app_code):
        logging.info('Initiating BtClientConfigBase...')
        self._app_config_repo = None
        self._app_code = app_code

    @property
    def app_config_repo(self):
        if not self._app_config_repo:
            self._app_config_repo = EtlAppConfigRepo()
        return self._app_config_repo

    @property
    def log_level(self):
        return self.cfg_dict.get(Code.LOG_LEVEL)

    @property
    def bt_req_code(self):
        return self.cfg_dict.get(Code.BT_REQ_CODE)

    @property
    def bt_endpoint(self):
        return self.cfg_dict.get(Code.BT_ENDPOINT)

    @cached_property
    def cfg_dict(self):
        codes = self.app_config_repo.list_by_app_code(self._app_code)
        return {Code.get(c.config_code): c.config_value for c in codes}

    def get_cfg_dict_by_key(self, key):
        return self.cfg_dict.get(key)

    @property
    def program_code(self):
        return self.cfg_dict.get(Code.BT_PROGRAM_CODE)

    @property
    def interface_code(self):
        return self.cfg_dict.get(Code.INTERFACE_CODE)

    @property
    def response_format_code(self):
        return self.cfg_dict.get(Code.BT_FORMAT)
