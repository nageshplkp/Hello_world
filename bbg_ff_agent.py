#!/usr/bin/env python

"""
Post request, gets response From Bloomberg Transport and downloads the CVS file.
"""

import getpass
import json
import logging
import os
import sys
from shutil import copyfile


from sqlalchemy.exc import DBAPIError

from core.rest.client import ClientException
from etl.bbg_transport.dto import RequestDataItem, RequestItem, RequestOptionItem
from etl.core.util import parse_args
from etl.core.util import uri_get, uri_post
from etl.enum.cor_da.gen.bt_status import BtStatusEnum
from etl.enum.pim_da.gen.bbg_interface import BbgInterfaceEnum
from etl.enum.pim_da.gen.bbg_program import BbgProgramEnum
from etl.enum.pim_da.gen.dats_batch_status import DatsBatchStatusEnum
from etl.repo.fnd_cfdw.etl_config import EtlConfigRepo
from etl.repo.pim_da.dats_bbg_batch import DatsBbgBatchRepo
from etl.repo.pim_da.dats_bbg_batch_series import DatsBbgBatchSeriesRepo

USAGE = ['BGG DATS  Automation Agent', ['action', {'help': 'REQUEST or POLL'}]]

BBG_ERROR = DatsBatchStatusEnum.BBG_ERROR.value
BT_DONE = DatsBatchStatusEnum.BT_DONE.value
BT_ERROR = DatsBatchStatusEnum.BT_ERROR.value
SENT_TO_BT = DatsBatchStatusEnum.SENT_TO_BT.value
IN_QUEUE = DatsBatchStatusEnum.IN_QUEUE.value
HTTP_ERROR = DatsBatchStatusEnum.HTTP_ERROR.value

BBGERROR = BtStatusEnum.BBGERROR.value
BTERROR = BtStatusEnum.BTERROR.value
INITIAL = BtStatusEnum.INITIAL.value
PENDING = BtStatusEnum.PENDING.value
SUCCESS = BtStatusEnum.SUCCESS.value

GETDATA = BbgProgramEnum.GETDATA.value
GETHISTORY = BbgProgramEnum.GETHISTORY.value

SAPI = BbgInterfaceEnum.SAPI.value


class FetcherAgent(object):
    """
        This class Does the Fetching process.
    """

    def __init__(self):
        self.log = logging.getLogger("{}".format(
            os.path.splitext(os.path.basename(__file__))[0]))
        self.username = getpass.getuser()
        logging.info('Fetching the required values from the EtlConfig Repo')

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        if exc_type is None:
            pass

    @staticmethod
    def _get_request(repo, option, dats_bt_actions):

        data = []
        try:
            if option == dats_bt_actions[0]:
                logging.info('Getting requests from dats_bbg_batch table with IN_QUEUE status')
                data = repo.list_by_batch_status_code(IN_QUEUE or BT_ERROR)
            elif option == dats_bt_actions[1]:
                logging.info('Getting requests from dats_bbg_batch '
                             'table with SENT_TO_BT')
                data = repo.list_by_batch_status_code(SENT_TO_BT)
            return data
        except DBAPIError as err:
            logging.error(err)


class RequestAgent(FetcherAgent):
    """
        This class posts request to Bloomberg Transport.
    """

    def _get_priority_list(self, results):

        logging.info('Prioritizing the list as per bbg_program_code '
                     'with GETDATA as high priority')
        data_list = []
        history_list = []
        for i in results:
            if i.bbg_program_code == GETDATA:
                data_list.append(i)
            else:
                history_list.append(i)
        lists = [data_list, history_list]
        data_list, history_list = [self._get_priority_list_by_interface_code(x) for x in lists]
        lists = data_list + history_list
        return lists

    @staticmethod
    def _get_priority_list_by_interface_code(results):

        logging.info('Re-Prioritizing the list as per bbg_interface_code,'
                     'with SAPI as high priority')
        plist = []
        for i in results:
            if i.bbg_interface_code == SAPI:
                plist.insert(0, i)
            else:
                plist.append(i)
        return plist

    def _get_data_items(self, obj, result_series):
        # Convert SQLAlchemy object to a dataframe as mentioned here.
        # http://pandas.pydata.org/pandas-docs/stable/whatsnew.html#whatsnew-0140-sql
        # http://pandas.pydata.org/pandas-docs/stable/generated/pandas.read_sql.html
        df = pandas.read_sql()  # Modify this according to your table.
        # Use your actual columns here.
        grouped_df = filtered_df.groupby(
                ['start_date', 'end_date', 'outer_grouper', 'inner_grouper'])
        for name, group in grouped_df:
            yield group.to_dict()

    def _get_request_object(self, obj, result_series):

        logging.info('Preparing the Request Object')
        data_items = self._get_data_items(obj, result_series)
        data_items = [RequestDataItem(tag=i.dats_code, bbg_query=i.bbg_query)
                      for i in result_series]
        # Get it just by doing fields = df.columns
        headers = self._get_headers(obj)
        fields = list(self._get_request_fields(result_series))
        # request_options = [RequestOptionItem(option_name=key, option_value=headers[key])
        #                    for key in headers]
        request = RequestItem(request_description=EtlConfigRepo.instance.
                              get_by_config_code('DATS_BT_DESCRIPTION').config_value,
                              requestor_code=EtlConfigRepo.instance.
                              get_by_config_code('DATS_BT_REQ_CODE').config_value,
                              program_code=obj.bbg_program_code,
                              interface_code=obj.bbg_interface_code,
                              response_format_code=EtlConfigRepo.instance.
                              get_by_config_code('DATS_BT_FORMAT').config_value,
                              request_data_items=data_items,
                              request_options=request_options,
                              request_fields=fields)
        logging.info('Converting the Request Object to json format')
        return json.dumps(request.to_json())

    @staticmethod
    def _get_headers(obj):

        headers = {}
        gh_range = str(obj.asof_start_date_key) + "|" + str(obj.asof_end_date_key)
        pc_dict = {GETDATA: [GETDATA.lower()],
                   GETHISTORY: [GETHISTORY.lower(), gh_range]}
        if obj.bbg_program_code == GETHISTORY:
            headers['DATERANGE'] = pc_dict[obj.bbg_program_code][1]
        headers['PROGRAMFLAG'] = EtlConfigRepo.instance. \
            get_by_config_code('PROGRAMFLAG').config_value
        headers['COMPRESS'] = EtlConfigRepo.instance. \
            get_by_config_code('COMPRESS').config_value
        headers['FIRMNAME'] = EtlConfigRepo.instance. \
            get_by_config_code('FIRMNAME').config_value
        headers['PROGRAMNAME'] = pc_dict[obj.bbg_program_code][0]
        headers['USERNUMBER'] = EtlConfigRepo.instance. \
            get_by_config_code('USERNUMBER').config_value
        return headers

    @staticmethod
    def _get_request_fields(result_series):

        request_fields_list = []
        for i in result_series:
            request_fields_list.append(i.mnemonic)

        return list(set(request_fields_list))

    def _post_to_bt(self, payload, end_point, obj, db_repo):

        logging.info('Sending the request to BT')
        logging.info('POST: %s, \r\n\t%s', end_point, payload)
        try:
            response = uri_post(end_point + 'request_data', payload)
            logging.info('response: %s \r\nresponse:\t%s', end_point, response)
            return response
        except ClientException as ex:
            logging.error(ex)
            self._update_request(obj.batch_id, None, None,
                                 payload, HTTP_ERROR, db_repo)

    @staticmethod
    def _update_request(batch_id, bt_request_id, bt_status_code,
                        request_obj, batch_status_code, repo):

        logging.info('Updating the staging the table')
        try:
            update_row = repo.get_by_batch_id(batch_id)
            update_row.batch_status_code = batch_status_code
            update_row.bt_request_id = bt_request_id
            update_row.bt_status_code = bt_status_code
            update_row.bt_request_payload = request_obj
            repo.save(update_row)
        except DBAPIError as ex:
            logging.error(ex)

    def run(self, action, ref_actions, end_point):
        """
        Get the response for all the requests and  update the table.
        """
        db_repo = DatsBbgBatchRepo()
        result = self._get_request(db_repo, action, ref_actions)
        priority_list = self._get_priority_list(result)
        repo = DatsBbgBatchSeriesRepo()
        for i in priority_list:
            logging.info("Fetching the records with batch_id" + ' ' + str(i.batch_id))
            try:
                result_batch = repo.list_by_batch_id(i.batch_id)
                obj = self._get_request_object(i, result_batch)
                response = self._post_to_bt(obj, end_point, i, db_repo)
                self._update_request(i.batch_id, response['request_id'],
                                     str(response['request_status']),
                                     str(obj), SENT_TO_BT, db_repo)
            except Exception as ex:
                logging.error(ex)


class ResponseAgent(FetcherAgent):
    """
        This class gets the response from the Bloomberg Transport.
    """

    @staticmethod
    def _get_request_status_by_url(obj, end_point):

        logging.info('Sending the check status request to BT')
        logging.info('GET: %s, \r\n\t%s', end_point, str(obj.bt_request_id))
        try:
            response = uri_get(end_point + 'check_status' + '/' + str(obj.bt_request_id))
            logging.info('response: %s \r\nresponse:\t%s', end_point, response)
            return response
        except Exception as ex:
            logging.error(ex)

    @staticmethod
    def _update_request(data_file_path, batch_id,
                        bt_status_code, repo):

        logging.info('Updating the staging the table')
        status_dict = {BBGERROR: BBG_ERROR,
                       BTERROR: BT_ERROR, SUCCESS: BT_DONE}
        try:
            update_row = repo.get_by_batch_id(batch_id)
            if bt_status_code not in [PENDING, INITIAL]:
                update_row.batch_status_code = status_dict[bt_status_code]
                update_row.bt_response_file_path = data_file_path
            update_row.bt_status_code = bt_status_code
            repo.save(update_row)
        except DBAPIError as err:
            logging.error(err)

    @staticmethod
    def copy_file(src, dst, program_code, batch_id):

        dats_bt_getdata_ext = EtlConfigRepo.instance. \
            get_by_config_code('DATS_BT_GETDATA_EXT').config_value
        dats_bt_gethis_ext = EtlConfigRepo.instance. \
            get_by_config_code('DATS_BT_GETHIS_EXT').config_value
        if not os.path.isdir(dst):
            logging.info("Can't copy %s to destination: %s", src, dst)
            raise Exception('Destination path is incorrect: %s', dst)
        destination_file = os.path.basename(src)
        date, req_id, ext = destination_file.split('.')
        if program_code == GETDATA:
            ext = dats_bt_getdata_ext + '.' + ext
        else:
            ext = dats_bt_gethis_ext + '.' + ext
        destination_file = 'BBG_' + date + '_' + \
                           str(batch_id) + '_' + req_id + ext
        dst = os.path.join(dst, destination_file)
        logging.info("Copying %s to %s", src, dst)
        copyfile(src, dst)

    def run(self, action, ref_actions, end_point):
        """
        Get the response for all the requests and  update the table.
        """
        dats_bt_file_path = EtlConfigRepo.instance. \
            get_by_config_code('DATS_BT_FILE_PATH').config_value
        db_repo = DatsBbgBatchRepo()
        result = self._get_request(db_repo, action, ref_actions)
        for i in result:
            response = self._get_request_status_by_url(i, end_point)
            if response['request_status'] == SUCCESS:
                self.copy_file(response['data_file_path'].strip(),
                               dats_bt_file_path, i.bbg_program_code, i.batch_id)
            self._update_request(response['data_file_path'], i.batch_id,
                                 response['request_status'], db_repo)


def main():
    logger = logging.getLogger("{}".format(os.path.splitext(os.path.basename(__file__))[0]))
    args = parse_args(*USAGE)
    user_action = args.action.upper()
    # Share data between agents.
    option = {'REQUEST': {'agent_name': 'Request Agent', 'agent_class': RequestAgent},
              'POLL': {'agent_name': 'Response Agent', 'agent_class': ResponseAgent}}
    if user_action in option.keys():
        try:
            dats_bt_req_action = EtlConfigRepo.instance. \
                get_by_config_code('DATS_BT_REQ_ACTION').config_value
            dats_bt_poll_action = EtlConfigRepo.instance. \
                get_by_config_code('DATS_BT_POLL_ACTION').config_value
            end_point = EtlConfigRepo.instance. \
                get_by_config_code('DATS_BT_ENDPOINT').config_value
            ref_actions = [dats_bt_req_action, dats_bt_poll_action]
            logging.info("%s started", option[user_action]['agent_name'])
            with option[user_action]['agent_class']() as agent:
                agent.run(user_action, ref_actions, end_point)
        except Exception as ex:
            logger.critical("%s exiting with error: %s", option[user_action]['agent_name'], ex)
            raise
        else:
            logger.info("%s Successfully completed.", option[user_action]['agent_name'])
    else:
        raise RuntimeError('Unknown action specified: {}'.format(user_action))
    return 0


if __name__ == "__main__":
    sys.exit(main())
