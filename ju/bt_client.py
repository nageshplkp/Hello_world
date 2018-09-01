import gzip
import io
import json
import logging
import numpy as np
from core.rest import rest_config
import sys
import time
from core.rest.client import ClientException
from dats_config import \
    BbgTransportForbidden, BbgTransportUnknown, BbgTransportErrorStatus,\
    BbgTransport404Status
from dats_repo import DatsProvider
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
    def _get_data_items(requests):
        provider = DatsProvider
        return [RequestDataItem(bbg_query=provider.to_bbg_query(r.req_ticker,
                                                                r.req_yellow_key,
                                                                r.req_overrides,
                                                                r.req_optional_elements),
                                tag=str(r.dats_bbg_request_id))
                for r in requests]

    def _get_fields(self, requests):
        req_mnemonics = [r.req_mnemonic for r in requests]
        req = np.unique(req_mnemonics)
        return np.append(req, self.bt_request_fields).tolist()

    def _options(self, has_pricing_source):
        options = [RequestOptionItem(key, val)
                   for (key, val) in self.bt_request_items.items()]
        if has_pricing_source:
            options.append(RequestOptionItem(option_name='EXCLUSIVE_PRICING_SRC',
                                             option_value='yes'))
        return options

    def get_request_item(self, requests, has_exclusive_pricing_source):
        data_items = self._get_data_items(requests)
        fields = self._get_fields(requests)
        options = self._options(has_exclusive_pricing_source)
        return RequestItem(request_description=self.request_description,
                           requestor_code=self.requester_code,
                           program_code=self.program_codes,
                           interface_code=self.interface_code,
                           response_format_code=self.response_format_code,
                           request_data_items=data_items,
                           request_options=options,
                           request_fields=fields)

    @timed()
    def post(self, req, has_pricing_source=False):
        """
        Call BT api [POST] and keep polling until response has complete status
        or times out
        :param req:
        :param has_pricing_source:
        :return:
        """
        logging.info('Submitting to BT...')
        payload = self.get_request_item(req, has_pricing_source).to_json()
        logging.info('payload size is {} bytes for {} requests'
                     .format(self.get_size(payload), len(req)))

        try:
            response = self._post(payload=payload)
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

    def _post(self, payload):
        logging.info('url: ' + self.base_url + 'request_data')
        return uri_post(url=self.base_url + 'request_data',
                        payload=payload) #,
                        # headers={'Content-Encoding': 'gzip'},
                        # request_serializer=self.stream_serializer)

    def stream_serializer(self, data, request_schema=None):
        """
        Stream Serializer will compress and stream
        :param data:
        :return:
        """
        zipped_payload = self._compress(data)
        logging.info('compressed payload size is {} bytes'
                     .format(self.get_size(zipped_payload)))
        return rest_config.MIME_JSON,\
               StreamingIterator(size=len(zipped_payload),
                                 iterator=self._gen(zipped_payload))

    @staticmethod
    def _compress(data):
        buf = io.BytesIO()
        with gzip.GzipFile(fileobj=buf, mode='wb') as f:
            json.dump(data, f)
        return buf.getvalue()

    @staticmethod
    def _gen(data):
        """
        chunk generator
        :param data:
        :return:
        """
        for i in range(0, len(data), 1000):
            yield data[i:i + 1000]

    def get(self, url):
        try:
            logging.info('url: ' + url)
            response = self._get(url)
        except ClientException as ex:
            status = ex.detail_json.get('status')
            logging.exception('BBG Transport API call failed: ' + ex.message)
            if status == 403:
                raise BbgTransportForbidden('Forbidden: ' +
                                            ex.detail_json.get('msg'))
            elif status == 404:
                raise BbgTransport404Status('Request Id not found')
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
                [i['error_text'] for i in response['response_file_info'] if
                 bool(i['is_error_response']) is True])
            raise BbgTransportErrorStatus('Bbg Transport Error: {}'
                                          .format(response['request_status'],
                                                  errors))
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
        elif hasattr(obj, '__iter__') and not isinstance(obj, (
        str, bytes, bytearray)):
            size += sum([self.get_size(i, seen) for i in obj])
        return size
