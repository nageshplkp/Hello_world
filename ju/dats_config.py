from bt_client_config import BtClientConfigBase
from etl.enum.cor_da.gen.bt_status import BtStatusEnum


# Exceptions
class BbgTransportUnknown(Exception):
    pass


class BbgTransportTimeout(Exception):
    pass


class BbgTransportForbidden(Exception):
    pass


class BbgTransportParserError(Exception):
    pass


class BbgTransportErrorStatus(Exception):
    pass


class DatsBtClientConfig(BtClientConfigBase):
    """
    This sub-class adds DATS specific configs on top of base config
    and is used by BT client.
    Create your own version of this.
    """
    """
    Validator Specific Configurations
    """
    CONFIG_APP_CODE = 'DATS_VALIDATION'
    AGENT_DIRECTION_REQUEST = 'REQUEST'
    AGENT_DIRECTION_POLLING = 'POLLING'

    def __init__(self):
        super(DatsBtClientConfig, self).__init__(self.CONFIG_APP_CODE)

    @staticmethod
    def bt_error_status():
        return [e.value for e in BtStatusEnum
                if 'ERROR' in e.value]

    @staticmethod
    def bt_complete_status():
        return [e.value for e in BtStatusEnum
                if e.value not in ['INITIAL', 'PENDING']]

    @property
    def bt_request_items(self):
        return {
            self.CODES.PROGRAMFLAG.value: self.cfg_dict.get(self.CODES.PROGRAMFLAG.value),
            self.CODES.PROGRAMNAME.value: self.cfg_dict.get(self.CODES.PROGRAMNAME.value),
            self.CODES.USERNUMBER.value: self.cfg_dict.get(self.CODES.USERNUMBER.value),
            self.CODES.FIRMNAME.value: self.cfg_dict.get(self.CODES.FIRMNAME.value)}

    @staticmethod
    def get_mandatory_mnemonics():
        return ['INDX_FREQ', 'PX_METHOD', 'LONG_COMP_NAME',
                'SECURITY_TYP', 'SECURITY_DES', 'TRADE_STATUS',
                'SECURITY_NAME', 'NAME',
                'TRADING_DAY_START_TIME_EOD',
                'TRADING_DAY_END_TIME_EOD',
                'LAST_UPDATE_DATE_EOD',
                'EXCHANGE_DELAY', 'PX_CLOSE_DT',
                'HISTORY_START_DT',
                'EQY_INIT_PO_DT', 'ISSUE_DT',
                'CPN_FREQ', 'FUND_PRICING_FREQ',
                'EXCH_CODE', 'TICKER', 'MARKET_SECTOR_DES',
                'PRICING_SOURCE', 'COUNTRY_ISO', 'MARKET_STATUS']
