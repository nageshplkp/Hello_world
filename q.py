import getpass
import logging
import time
from etl.core.util import uri_post
from etl.repo.pim_pm.pl_bbg_batch import PlBbgBatchRepo
from etl.repo.pim_pm.pl_bbg_batch_series_vw import PlBbgBatchSeriesVwRepo

BASE_URL = 'http://ptp-dev/workshop/service/da/bbg_transport/'


class QueuerAgent:

    def __init__(self):
        logging.info('QueuerAgent')
        self.USERNAME = getpass.getuser()

    @staticmethod
    def getrequests(repo):
        model = repo.model
        data = repo.query.filter(model.batch_status_code == 'IN_QUEUE').all()
        return data

    def getprioritylist(result):
        plist= []
        for i in result:
            if i.bbg_program_code == 'GETDATA':
                plist.insert(0,i)
            elif i.bbg_program_code == 'GETHISTORY':
                plist.append(i)
            else:
                pass
        return plist

    @staticmethod
    def get_request_object(objdata, result_series):
        payload = dict()
        payload["requestor_code"] = 'DA.PIMCOLIVE.DEV'
        payload["request_description"] = 'TEST'
        payload["program_code"] = objdata.bbg_program_code
        payload["interface_code"] = objdata.bbg_interface_code
        payload["response_format_code"] = 'HORIZONTAL'
        items_list = []
        request_fields_list = []
        for i in result_series:
            element = dict()
            element["cusip"] = ""
            element["yellow_key"] = i.bbg_yellow
            element["bb_id"] = ""
            element["tag"] = i.pl_series_code
            element["isin"] = ""
            element["bbg_query"] = ""
            element["ticker"] = i.bbg_ticker
            items_list.append(element)
            request_fields_list.append(i.bbg_mnemonic)
        payload["request_data_items"] = items_list
        payload["request_fields"] = list(set(request_fields_list))
        req_options=[]
        req_options_item=dict()
        req_options_item["option_name"]='DATERANGE'
        if objdata.bbg_program_code == 'GETDATA':
            req_options_item["option_name"] = time.strftime('%Y%m%d')
        elif objdata.bbg_program_code == 'GETHISTORY':
            req_options_item["option_name"] = objdata.start_date + "|" + objdata.end_date
        payload["request_options"] = req_options
        return payload

    @staticmethod
    def post_to_bt(payload):
        response = uri_post(BASE_URL + 'request_data', payload)
        return response

    @staticmethod
    def update_request(batch_id, bt_request_id, bt_status_code, request_obj, status_code, repo):
        model = repo.model
        update_row = repo.query.filter(model.batch_status_code == 'IN_QUEUE', model.batch_id == batch_id).all()
        update_row[0].batch_status_code = status_code
        update_row[0].bt_request_id = bt_request_id
        update_row[0].bt_status_code = bt_status_code
        update_row[0].bt_request_payload = request_obj
        repo.save(update_row)

    def run(self):
        result = self.getrequests(PlBbgBatchRepo())
        priority_list= self.getprioritylist(result)
        for i in priority_list:
            repo = PlBbgBatchSeriesVwRepo()
            model = repo.model
            result_batch = repo.query.filter(model.batch_id == i.batch_id).all()
            obj = self.get_request_object(i, result_batch)
            response = self.post_to_bt(obj)
            self.update_request(i.batch_id, response['request_id'],
                                   str(response['request_status']), str(obj), 'SENT_TO_BT', PlBbgBatchRepo())


if __name__ == '__main__':
    agent = QueuerAgent()
    agent.run()