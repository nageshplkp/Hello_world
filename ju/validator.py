import abc
import logging
import os
import sys
from collections import namedtuple
from contextlib import contextmanager

import numpy as np
import pandas as pd

from bt_client import BtClient
from dats_config import DatsBtClientConfig as ValidatorConfig, \
    BbgTransportErrorStatus, BbgTransport404Status
from dats_repo import DatsProvider, DatsBbgRequest
from etl.core.timed import timed
from etl.core.util import parse_args
from etl.enum.pim_da.gen.dats_bbg_request_status import DatsBbgRequestStatusEnum

USAGE = ['DATS BBG Validate', ['direction', {'help': 'REQUEST or POLL'}]]
HEADER_PREFIX = 'BBG_'
HAS_PRICING_SOURCES = [True, False]
Status = namedtuple('Status', ['bt_request_id', 'result'])


class ValidatorAgent:
    __metaclass__ = abc.ABCMeta

    def __init__(self, processing_status, req_limit_key):
        super(ValidatorAgent, self).__init__()
        self._config = ValidatorConfig()
        logging.getLogger(__name__).setLevel(self._config.log_level)
        self._processing_status = processing_status
        self._request_limit = self._config.get_cfg_dict_by_key(req_limit_key)
        self._provider = None
        self._bt_client = None
        self._requests = None
        self._result = ValidatorConfig.NO_ITEMS_PROCESSED

    def __enter__(self):
        logging.info('--enter--')

    def __exit__(self, exc_type, exc_value, traceback):
        """
        Log result
        :return:
        """
        logging.info('--exit--')

    @property
    def bt_client(self):
        if self._bt_client is None:
            self._bt_client = BtClient(config=self._config)
        return self._bt_client

    @property
    def provider(self):
        if self._provider is None:
            self._provider = DatsProvider()
        return self._provider

    @abc.abstractmethod
    def get_requests_from_db(self, get_pricing_source):
        pass

    def update_requests(self, status_to, bt_request_id=None):
        """
        Update the Request table with the response from BT
        :param status_to:
        :param bt_request_id:
        :return:
        """
        try:
            logging.info('Updating request status in DB')
            self.provider.update_dats_bbg_request(self._requests, status_to,
                                                  bt_request_id)
        except Exception as err:
            logging.exception(
                'Error at ValidatorRequestAgent.update_requests: ' +
                err.message)
            return False
        else:
            return True

    @abc.abstractmethod
    def execute(self):
        pass


class ValidatorRequestAgent(ValidatorAgent):
    def __init__(self):
        super(ValidatorRequestAgent, self). \
            __init__(DatsBbgRequestStatusEnum.NEW,
                     ValidatorConfig.CODES.REQUEST_LIMIT)
        self._response = None
        self._exclusive_pricing_source_enum = [True, False]

    @contextmanager
    def execute(self):
        """
        Retrieve new request from DB, submit request to BT, update status
        :return:
        """
        for ps in HAS_PRICING_SOURCES:
            self.get_requests_from_db(ps)
            if self.is_requests_found:
                self.submit_request(ps)
                if self.is_response_good:
                    logging.info('Successfully submitted')
                    is_done = self.update_requests(
                        DatsBbgRequestStatusEnum.PENDING,
                        self._response['request_id'])
                    if is_done:
                        logging.info('Successfully updated')
                        self._result = ValidatorConfig.ITEMS_PROCESSED
        yield self._result

    def get_requests_from_db(self, has_pricing_source):
        """
        Get requests from DA_OWN.DATS_BBG_REQUEST table
        """
        try:
            logging.info(
                'Retrieving requests from DATS_BBG_REQUEST table, max {} req''s'
                    .format(self._request_limit))
            requests = self.provider \
                .list_dats_bbg_request_by_status(self._processing_status,
                                                 self._request_limit,
                                                 has_pricing_source)
            self._requests = requests
            logging.info(
                'Retrieved {} requests'.format(len(requests)))
        except Exception as err:
            logging.exception(err.message)

    @property
    def is_response_good(self):
        return self._response and not self._response.get('is_error')

    @property
    def is_requests_found(self):
        return self._requests and len(self._requests) > 0

    def submit_request(self, has_pricing_source):
        """
        Call BBG Transport, add to BT request queue
        :return:
        """
        try:
            logging.info('Submitting validation requests to BBG Transport..')
            self._response = self.bt_client.post(self._requests,
                                                 has_pricing_source)
        except Exception as err:
            logging.exception(err.message)


class ValidatorPollingAgent(ValidatorAgent):
    def __init__(self):
        super(ValidatorPollingAgent, self). \
            __init__(DatsBbgRequestStatusEnum.PENDING,
                     ValidatorConfig.CODES.POLL_LIMIT)
        self._request_df = None

        # current bt_request_id
        self._bt_request_id = None
        # _current_requests is a subset of self._requests for the bt_request_id
        self._current_requests = None

        self._status_list = []
        self.result = None

    @property
    def bt_request_id(self):
        if self._bt_request_id is not None:
            return str(self._bt_request_id)
        return ''

    def set_bt_request_id(self, bt_request_id):
        self._bt_request_id = str(bt_request_id)

    @contextmanager
    def execute(self):
        bt_request_ids = self.get_requests_from_db()
        if bt_request_ids is None:
            yield self._result
        else:
            self.process_each_request(bt_request_ids)
            yield self._result

    def process_each_request(self, bt_request_ids):
        for each_request_id in bt_request_ids:
            self.set_working_batch(each_request_id)
            self.transform_request_to_df()
            try:
                if self.has_data_from_bt():
                    result = self.validate()
                else:
                    result = False
                self.save_response_to_db()
                self._result = ValidatorConfig.ITEMS_PROCESSED
            except BbgTransportErrorStatus:
                result = False
            self.add_to_result_list(self._bt_request_id, result)

    def set_working_batch(self, each_request_id):
        # reset _bt_request_id, _requests to per req. item
        self.set_bt_request_id(each_request_id)
        self._current_requests = [r for r in self._requests
                                  if r.bt_request_id == each_request_id]

    def add_to_result_list(self, bt_request_id, is_success):
        logging.info('Adding Validation Result for {bt_request_id}: [{result}]'
                     .format(bt_request_id=bt_request_id,
                             result=str(is_success)))
        self._status_list.append(Status(bt_request_id=bt_request_id,
                                        result=is_success))

    def get_requests_from_db(self, ps=None):
        """
        Get requests from DA_OWN.DATS_BBG_REQUEST table
        """
        try:
            logging.info(
                'Retrieving requests from DATS_BBG_REQUEST table..')
            requests = self.provider \
                .get_by_bt_request_id(self._processing_status,
                                      self._request_limit)
            self._requests = requests
            return self.get_unique_request_ids(self._requests)
        except Exception as err:
            logging.exception(err.message)

    @staticmethod
    def get_unique_request_ids(requests):
        if requests:
            return np.unique([r.bt_request_id for r in requests])

    def transform_request_to_df(self):
        reqs = [DatsBbgRequest(dats_bbg_request_id=r.dats_bbg_request_id,
                               dats_bbg_request_status_code=r.dats_bbg_request_status_code,
                               req_ticker=r.req_ticker,
                               req_yellow_key=r.req_yellow_key,
                               req_mnemonic=r.req_mnemonic,
                               req_bbg_interface_code=r.req_bbg_interface_code,
                               req_pricing_source=r.req_pricing_source,
                               req_overrides=r.req_overrides,
                               req_optional_elements=r.req_optional_elements,
                               req_tag=r.req_tag,
                               req_is_register_series=r.req_is_register_series,
                               bt_request_id=r.bt_request_id).to_dict()
                for r in self._current_requests]
        self._request_df = pd.DataFrame.from_records(reqs)

    def has_data_from_bt(self):
        try:
            status_url = self.bt_client.get_status_url(self._bt_request_id)
            logging.info('Checking status for {bt_id}...'
                         .format(bt_id=self._bt_request_id))
            response = self.bt_client.get(url=status_url)
            if self.is_completed(response):
                self.bt_client.handle_response(response)
                response_url = self.bt_client.get_response_url(
                    self._bt_request_id)
                logging.info('Getting response for {bt_id}...'
                             .format(bt_id=self._bt_request_id))
                resp = self.bt_client.get(url=response_url)
                self.bt_client.handle_response(resp)
                self.read_and_build_dataframe(resp)
        except BbgTransport404Status as ex404:
            logging.error('Not Found: ' + ex404.message)
            self.provider.update_dats_bbg_request(self._current_requests,
                                                  DatsBbgRequestStatusEnum.ERROR,
                                                  self._bt_request_id,
                                                  'BT Request Id {} is not found'.format(
                                                      self._bt_request_id))
        except BbgTransportErrorStatus as ex:
            logging.error('BT or BBG error: ' + ex.message)
            self.provider.update_dats_bbg_request(self._current_requests,
                                                  DatsBbgRequestStatusEnum.ERROR,
                                                  self._bt_request_id,
                                                  ex.message)
            return False
        except Exception as ex:
            logging.error('Parse failed: ' + ex.message)
            return False
        else:
            return True

    @staticmethod
    def is_completed(response):
        if response:
            logging.info('Status is {status}...'
                         .format(status=response['request_status']))
            return response.get(
                'request_status') in ValidatorConfig.bt_complete_status()

    def read_and_build_dataframe(self, resp):
        logging.info('Dataframe conversion...')

        bbg_resp_df = pd.DataFrame(resp['data'])
        self.rename_column_bbg_prefix(bbg_resp_df)
        self.extract_identifier_from_tag(bbg_resp_df)
        self._request_df = self.join_request_and_response_df(self._request_df,
                                                             bbg_resp_df)
        self.find_mnemonic_value(self._request_df)
        self._request_df['STATUS'] = DatsBbgRequestStatusEnum.PENDING.value
        logging.info('Dataframe conversion finished...')

    @staticmethod
    def find_mnemonic_value(request_df):
        # find the right column to look at the value of mnemonic
        request_df['col_no'] = request_df['req_mnemonic'].apply(
            lambda r: request_df.columns.get_loc(HEADER_PREFIX + r))
        # get the right mnemonic value and fill in 'return_val' column
        for i, w in request_df['col_no'].iteritems():
            request_df.loc[i, 'return_val'] = request_df.iloc[i, w]

    @staticmethod
    def join_request_and_response_df(request_df, bbg_series):
        request_df = pd.merge(request_df, bbg_series, how='inner',
                              left_on=['dats_bbg_request_id'],
                              right_on=['request_id'])
        return request_df

    @staticmethod
    def extract_identifier_from_tag(bbg_series):
        bbg_series['request_id'] = pd.to_numeric(
            bbg_series[HEADER_PREFIX + 'REQUESTOR_TAG'].str.replace('##', ''))

    @staticmethod
    def rename_column_bbg_prefix(bbg_series):
        names = {col_name: HEADER_PREFIX + col_name for col_name in
                 list(bbg_series.columns.values)}
        bbg_series.rename(columns=names, inplace=True)

    @timed()
    def validate(self):
        """
        Validate the bbg series pandas dataframe
        :return:
        """
        try:
            logging.info('Validating...')
            logging.info('Validating for INVALID status...')
            df = self._request_df
            ValidationUtility.validate_identifier(df,
                                                  self.provider.get_getdata_desc_of)
            ValidationUtility.validate_mnemonic(df)
            ValidationUtility.validate_yellow_key(df)
            ValidationUtility.validate_pricing_source(df)
            logging.info('Validating for ONHOLD_REG status...')
            ValidationUtility.update_onhold_reg_status(df)
            logging.info('Validation finished...')
            ValidationUtility.update_validated_status(df)
            return True
        except Exception as err:
            logging.error('ValidatorPollingAgent.validate(): ' + err.message)
            return False

    def save_response_to_db(self):
        logging.info('Saving to database...')
        try:
            self.clean_up_df(self._request_df)
            self.provider.save_dats_bbg_request(self._request_df)
        except Exception as ex:
            logging.error('Save to DB failed: ' + ex.message)
        else:
            logging.info('Successfully saved.')

    @staticmethod
    def clean_up_df(df):
        # set status
        df['dats_bbg_request_status_code'] = df['STATUS']
        # drop temp. cols
        df.drop([HEADER_PREFIX + 'REQUESTOR_TAG',
                 'BBG_ROW_STATUS',
                 'BBG_NUM_OF_FIELDS',
                 'BBG_SECURITY',
                 'request_id',
                 'col_no',
                 'return_val',
                 'STATUS'],
                axis=1, inplace=True)
        df['BBG_CPN_FREQ'] = np.where(df['BBG_CPN_FREQ'] == '', None,
                                      df['BBG_CPN_FREQ'])
        # drop request mnemonic cols
        req_mnemonics = df.req_mnemonic.unique()
        req_mnemonics = np.vectorize(lambda r: HEADER_PREFIX + r)(req_mnemonics)
        df.drop(req_mnemonics.tolist(), axis=1, inplace=True)


class ValidationUtility:
    def __init__(self):
        pass

    @staticmethod
    def validate_identifier(bbg_df, get_getdata_desc_of):
        """
        check if row status is anything other than 0
        :return:
        """
        logging.info('Validate Identifiers...')
        bbg_df.loc[bbg_df.BBG_ROW_STATUS.astype(int) != 0, 'STATUS'] = \
            DatsBbgRequestStatusEnum.INVALID.value
        bbg_df.loc[bbg_df.BBG_ROW_STATUS.astype(int) != 0, 'public_msg'] = \
            bbg_df.loc[bbg_df.BBG_ROW_STATUS.astype(int) != 0,
                       'BBG_ROW_STATUS'].apply(
                lambda r: get_getdata_desc_of(int(r)))

    @staticmethod
    def validate_mnemonic(bbg_df):
        """
        # the column in question contains the value
        :return:
        """
        logging.info('Validate Mnemonic...')
        bbg_df.loc[bbg_df.return_val == 'FLD UNKNOWN',
                   'STATUS'] = DatsBbgRequestStatusEnum.INVALID.value
        bbg_df.loc[bbg_df.return_val == 'FLD UNKNOWN',
                   'public_msg'] = 'Invalid BBG Mnemonic'

    @staticmethod
    def validate_yellow_key(bbg_df):
        logging.info('Validate yellow key...')
        where = (bbg_df['STATUS'] != DatsBbgRequestStatusEnum.INVALID.value) & \
                (bbg_df['req_yellow_key'] != bbg_df['BBG_MARKET_SECTOR_DES'])
        bbg_df.loc[where, 'STATUS'] = DatsBbgRequestStatusEnum.INVALID.value
        bbg_df.loc[
            where, 'public_msg'] = 'Requested yellow is not matching with ' \
                                   'bbg return yellow key'

    @staticmethod
    def update_onhold_reg_status(bbg_df):
        logging.info('Validate on-hold register...')
        where = (bbg_df['STATUS'] != DatsBbgRequestStatusEnum.INVALID.value) & \
                (bbg_df['req_is_register_series'] == 0)
        bbg_df.loc[where, 'STATUS'] = DatsBbgRequestStatusEnum.ONHOLD_REG.value
        bbg_df.loc[
            where, 'public_msg'] = 'Request submitted with registration onhold'

    @staticmethod
    def validate_pricing_source(bbg_df):
        logging.info('Validate pricing source...')
        where = (bbg_df['STATUS'] != DatsBbgRequestStatusEnum.INVALID.value) & \
                (bbg_df.req_pricing_source is not None and
                 bbg_df.BBG_PRICING_SOURCE != '') & \
                (bbg_df.req_pricing_source != bbg_df.BBG_PRICING_SOURCE)

        bbg_df.loc[where, 'STATUS'] = DatsBbgRequestStatusEnum.INVALID.value
        bbg_df.loc[
            where, 'public_msg'] = 'Requested pricing source is different ' \
                                   'from bbg return pricing source'

    @staticmethod
    def update_validated_status(bbg_df):
        where = (bbg_df['STATUS'] != DatsBbgRequestStatusEnum.INVALID.value)
        bbg_df.loc[where, 'STATUS'] = DatsBbgRequestStatusEnum.VALID.value
        bbg_df.loc[where, 'public_msg'] = 'Series successfully processed'


if __name__ == '__main__':
    direction = parse_args(*USAGE).direction
    os.environ['PYPIMCO_LOG_EX_ETL_AUDIT_ID'] = os.environ.get('ETL_AUDIT_ID')
    if direction.upper() == ValidatorConfig.AGENT_DIRECTION_REQUEST:
        agent = ValidatorRequestAgent()
    elif direction.upper() == ValidatorConfig.AGENT_DIRECTION_POLL:
        agent = ValidatorPollingAgent()
    else:
        raise RuntimeError('Unknown direction specified: {}'.format(direction))
    with agent.execute() as x:
        sys.exit(x)
