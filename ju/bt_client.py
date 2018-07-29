import gzip
import io
import json
import logging
import numpy as np
import sys
import time
from dats_config import \
    BbgTransportForbidden, BbgTransportUnknown, BbgTransportErrorStatus
from core.rest.client import ClientException
from etl.core.util import uri_post, uri_get
from etl.core.timed import timed
from etl.bbg_transport.dto import RequestDataItem, RequestItem, \
    RequestOptionItem
from requests_toolbelt.streaming_iterator import StreamingIterator


class BtClient:
    def __init__(self, config):
        # set common BT client config
        self.request_description = 'Get Data'

        bt_config = config
        self.base_url = bt_config.bt_endpoint
        self.requester_code = bt_config.bt_req_code
        # set DATS specific configs. it comes from the config subclass instance
        self.bt_request_items = bt_config.bt_request_items
        self.program_codes = bt_config.program_code
        self.interface_code = bt_config.interface_code
        self.response_format_code = bt_config.response_format_code
        # enum or internal settings - static methods from abc
        self.bt_request_fields = bt_config.get_mandatory_mnemonics()
        self.bt_error_status = bt_config.bt_error_status()
        self.bt_complete_status = bt_config.bt_complete_status()

    def __enter__(self):
        self.start_time = time.time()

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Display auditing details
        :param exc_type:
        :param exc_val:
        :param exc_tb:
        :return:
        """
        self.end_time = time.time()
        elapsed_time = self.end_time - self.start_time
        logging.info('Overall time elapsed: %ss', elapsed_time)
        logging.info('%s completed at %s', self.__class__.__name__,
                     self.end_time)

    @staticmethod
    def _get_data_items(dats_bbg_requests):
        return [RequestDataItem(bbg_query=r.req_ticker + ' ' + r.req_yellow_key,
                                tag=str(r.dats_bbg_request_id))
                for r in dats_bbg_requests]

    def _get_fields(self, requests):
        req_mnemonics = [r.req_mnemonic for r in requests]
        req = np.unique(req_mnemonics)
        return np.append(req, self.bt_request_fields).tolist()

    @property
    def _options(self):
        return [RequestOptionItem(key, val)
                for (key, val) in self.bt_request_items.items()]

    def get_request_item(self, req):
        data_items = self._get_data_items(req)
        fields = self._get_fields(req)
        options = self._options
        return RequestItem(request_description=self.request_description,
                           requestor_code=self.requester_code,
                           program_code=self.program_codes,
                           interface_code=self.interface_code,
                           response_format_code=self.response_format_code,
                           request_data_items=data_items,
                           request_options=options,
                           request_fields=fields)

    @timed()
    def post_to_bbg_transport(self, req):
        """
        Call BT api [POST] and keep polling until response has complete status
        or times out
        :param req:
        :return:
        """
        logging.info('Submitting to BT...')

        json_payload = self.get_request_item(req).to_json()
        logging.info('payload size is {} bytes for {} requests'
                     .format(self.get_size(json_payload), len(req)))

        buf = io.BytesIO()
        with gzip.GzipFile(fileobj=buf, mode='wb') as f:
            # todo: fix this to use pypimco serialize.py
            json.dump(json_payload, f)

        zipped_payload = buf.getvalue()

        logging.info('compressed payload size is {} bytes for {} requests'
                     .format(self.get_size(zipped_payload), len(req)))

        payload = zipped_payload

        def gen(data):
            for i in range(0, len(data), 1000):
                yield data[i:i+1000]

        streamer = StreamingIterator(size=len(payload), iterator=gen(payload))

        try:
            response = self._post(data=streamer,
                                  headers={'Content-Type': 'application/json',
                                           'Content-Encoding': 'gzip'},
                                  is_stream=True)
        except ClientException as ex:
            status = ex.detail_json.get('status')
            logging.exception('BBG Transport API call failed: ' + ex.message)
            if status == 403:
                raise BbgTransportForbidden('Forbidden: ' +
                                            ex.detail_json.get('msg'))
        except Exception as ex:
            logging.exception('BBG Transport Server Error: ' + ex.message)
            raise
        else:
            return self.handle_response(response)

    def _post(self, data, headers=None, is_stream=False):
        # todo: fix this after PyPimco client change
        return uri_post(url=self.base_url + 'request_data', payload=data,
                        headers=headers)
        # return uri_post(url=self.base_url + 'request_data', payload=data,
        #                 headers=headers, is_stream=is_stream)

    def get_from_bbg_transport(self, bt_url):
        try:
            response = self._get(bt_url)
        except ClientException as ex:
            status = ex.detail_json.get('status')
            logging.exception('BBG Transport API call failed: ' + ex.message)
            if status == 403:
                raise BbgTransportForbidden('Forbidden: ' +
                                            ex.detail_json.get('msg'))
        else:
            return self.handle_response(response)

    def get_response_url(self, bt_request_id):
        return self.base_url + 'response/' + bt_request_id

    def get_status_url(self, bt_request_id):
        return self.base_url + 'check_status/' + bt_request_id

    @staticmethod
    def _get(url):
        return uri_get(url)

    def handle_response(self, response):
        """
        handle response
        :param response:
        :return:
        """
        if response is None:
            raise BbgTransportUnknown('Response is None')

        if response['request_status'] in self.bt_error_status:
            errors = '\n'.join(
                [i.error_text for i in response['response_file_info'] if
                 bool(i['is_error_response']) is True])
            raise BbgTransportErrorStatus('Bbg Transport Error: {}'
                                          .format(response['request_status'], errors))
        return response

    def get_size(self, obj, seen=None):
        size = sys.getsizeof(obj)
        if seen is None:
            seen = set()
        obj_id = id(obj)
        if obj_id in seen:
            return 0
        seen.add(obj_id)
        if isinstance(obj, dict):
            size += sum([self.get_size(v, seen) for v in obj.values()])
            size += sum([self.get_size(k, seen) for k in obj.keys()])
        elif hasattr(obj, '__dict__'):
            size += self.get_size(obj.__dict__, seen)
        elif hasattr(obj, '__iter__') and not isinstance(obj, (str, bytes, bytearray)):
            size += sum([self.get_size(i, seen) for i in obj])
        return size
