import logging
import numpy as np
from werkzeug.utils import cached_property
from etl.repo.pim_da.bbg_return_status import BbgReturnStatusRepo
from etl.repo.pim_da.dats_bbg_request import DatsBbgRequestRepo
from etl.repo.pim_da.dats_series import DatsSeriesRepo
from etl.repo.pim_da.dats_series_bbg import DatsSeriesBbgRepo
from etl.repo.pim_da.dats_series_tss_meta import DatsSeriesTssMetaRepo
from etl.repo.pim_da.dats_tss_field_rule import DatsTssFieldRuleRepo
PROVIDER = 'BBG'


class DatsRepoBase(object):
    def __init__(self):
        self._sqla_logger = logging.getLogger('sqlalchemy')
        self._dats_bbg_request_repo = None
        self._dats_series_repo = None
        self._dats_series_bbg_repo = None
        self._dats_series_tss_meta_repo = None
        self._dats_tss_field_rule = None
        self._bbg_return_status_repo = None

    @property
    def _bbg_return_status_repo_(self):
        if self._bbg_return_status_repo is None:
            self._bbg_return_status_repo = BbgReturnStatusRepo()
        return self._bbg_return_status_repo

    @property
    def _dats_tss_field_rule_(self):
        if self._dats_tss_field_rule is None:
            self._dats_tss_field_rule = DatsTssFieldRuleRepo()
        return self._dats_tss_field_rule

    @property
    def _dats_bbg_request_repo_(self):
        if self._dats_bbg_request_repo is None:
            self._dats_bbg_request_repo = DatsBbgRequestRepo()
        return self._dats_bbg_request_repo

    @property
    def _dats_series_tss_meta_repo_(self):
        if self._dats_series_tss_meta_repo is None:
            self._dats_series_tss_meta_repo = DatsSeriesTssMetaRepo()
        return self._dats_series_tss_meta_repo

    @property
    def _dats_series_repo_(self):
        if self._dats_series_repo is None:
            self._dats_series_repo = DatsSeriesRepo()
        return self._dats_series_repo

    @property
    def _dats_series_bbg_repo_(self):
        if self._dats_series_bbg_repo is None:
            self._dats_series_bbg_repo = DatsSeriesBbgRepo()
        return self._dats_series_bbg_repo


class DatsBbgRequest:
    def __init__(self, dats_bbg_request_id=None, dats_bbg_request_status_code=None,
                 req_ticker=None, req_yellow_key=None, req_mnemonic=None,
                 req_bbg_interface_code=None, req_pricing_source=None,
                 req_overrides=None, req_optional_elements=None, req_tag=None,
                 req_is_register_series=None, bt_request_id=None):
        self.dats_bbg_request_id = dats_bbg_request_id
        self.dats_bbg_request_status_code = dats_bbg_request_status_code
        self.req_ticker = req_ticker
        self.req_yellow_key = req_yellow_key
        self.req_mnemonic = req_mnemonic
        self.req_bbg_interface_code = req_bbg_interface_code
        self.req_pricing_source = req_pricing_source
        self.req_overrides = req_overrides
        self.req_optional_elements = req_optional_elements
        self.req_is_register_series = req_is_register_series
        self.req_tag = req_tag
        self.bt_request_id = bt_request_id

    def to_dict(self):
        return {
            'dats_bbg_request_id': self.dats_bbg_request_id,
            'dats_bbg_request_status_code': self.dats_bbg_request_status_code,
            'req_ticker': self.req_ticker,
            'req_yellow_key': self.req_yellow_key,
            'req_mnemonic': self.req_mnemonic,
            'req_bbg_interface_code': self.req_bbg_interface_code,
            'req_pricing_source': self.req_pricing_source,
            'req_overrides': self.req_overrides,
            'req_optional_elements': self.req_optional_elements,
            'req_is_register_series': self.req_is_register_series,
            'req_tag': self.req_tag,
            'bt_request_id': self.bt_request_id
        }


class BbgReturnStatus:
    def __init__(self, bbg_return_status_code=None, bbg_getdata_descr=None,
                 bbg_gethistory_descr=None):
        self.bbg_return_status_code = bbg_return_status_code
        self.bbg_getdata_descr = bbg_getdata_descr
        self.bbg_gethistory_descr = bbg_gethistory_descr


class DatsProvider(DatsRepoBase):

    def __init__(self):
        super(DatsProvider, self).__init__()

    @staticmethod
    def to_bbg_query(req_ticker, req_yellow_key, req_overrides='UND',
                     req_optional_elements='UND'):
        query = req_ticker + ' ' + req_yellow_key
        if req_overrides != 'UND' and len(req_overrides) > 1:
            query = query + '|' + req_overrides
        if req_optional_elements != 'UND' and len(req_optional_elements) > 1:
            query = query + '|' + req_optional_elements
        return query

    def to_dats_code(self, req_ticker, req_yellow_key, req_mnemonic):
        return (self.to_bbg_query(req_ticker, req_yellow_key)
                + ' ' + req_mnemonic + ' ' + PROVIDER).replace(' ', '-')

    @cached_property
    def list_bbg_return_status(self):
        all_status = self._bbg_return_status_repo_.list_all()
        return {int(status.bbg_return_status_code):
                BbgReturnStatus(status.bbg_return_status_code,
                                status.bbg_getdata_descr,
                                status.bbg_gethistory_descr) for status in
                all_status}

    def get_getdata_desc_of(self, code):
        return self.list_bbg_return_status[code].bbg_getdata_descr

    def get_gethistory_desc_of(self, code):
        return self.list_bbg_return_status[code].bbg_gethistory_descr

    def save_dats_bbg_request(self, requests):
        dict_requests = requests.to_dict('records')
        dict_reqs_lower = [{k.lower(): v for k, v in d.items()}
                           for d in dict_requests]

        self._dats_bbg_request_repo_.update_all(dict_reqs_lower)

    def update_dats_bbg_request(self, requests, status_to,
                                bt_request_id, public_msg=None):
        # apply status to all requests
        request_ids = np.array([r.dats_bbg_request_id for r in requests])
        self._dats_bbg_request_repo_.update(request_ids, status_to,
                                            bt_request_id, public_msg)

    def list_dats_bbg_request_by_status(self, status, num, has_pricing_source):
        if has_pricing_source:
            requests = self._dats_bbg_request_repo_. \
                list_by_dats_bbg_request_status_code_limit_has_ps(status.value, num).all()
        else:
            requests = self._dats_bbg_request_repo_. \
                list_by_dats_bbg_request_status_code_limit_no_ps(status.value, num).all()
        return requests

    def get_by_bt_request_id(self, status, limit):
        requests = self._dats_bbg_request_repo_. \
            list_by_bt_request_id(status.value, limit).all()
        return requests

