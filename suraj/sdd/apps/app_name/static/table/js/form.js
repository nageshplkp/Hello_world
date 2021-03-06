var bbg_transport;
(function (bbg_transport) {
    var Form = /** @class */ (function (_super) {
        __extends(Form, _super);
        function Form(el) {
            var _this = _super.call(this, el) || this;
            _this.actionCheckRequestStatus = function (inputValues, errors) {
                var action = inputValues['action'];
                switch (action) {
                    case 'check_status':
                    case 'response':
                        break;
                    default:
                        return false;
                }
                var requestId = inputValues['request_id'];
                _this.ajaxGet(Form.serviceBaseUrl + '/' + action + '/' + requestId);
                return true;
            };
            _this.actionCreateRequest = function (inputValues, errors) {
                var programCode = inputValues['program_code'];
                var requestOptions = inputValues['bbg_transport_CreateRequestOption_request_id'];
                if (programCode === 'GETHISTORY' && requestOptions.length === 0) {
                    errors.push('GETHISTORY requires least 1 Request Option.');
                }
                var requestDataItems = inputValues['bbg_transport_CreateRequestDataItem_request_id'];
                if (requestDataItems.length === 0) {
                    errors.push('At least 1 Request Data Item is required.');
                }
                else {
                    var dataItems = inputValues['bbg_transport_CreateRequestDataItem_request_id'].map(function (o) {
                        var item = {
                            'yellow_key': o['yellow_key'],
                            'ticker': o['ticker'],
                            'bbg_query': o['bbg_query'],
                            'isin': o['isin'],
                            'cusip': o['cusip'],
                            'bb_id': o['bb_id'],
                            'tag': o['tag'],
                        };
                        if (item.yellow_key !== '' && item.ticker === '') {
                            errors.push('Ticker is required when Yellow Key is specified for a Request Data Item.');
                        }
                        else if (item.yellow_key === '' && item.ticker !== '') {
                            errors.push('Yellow Key is required when Ticker is specified for a Request Data Item.');
                        }
                        else {
                            const keys = Object.keys(item)
                                for (const key of keys) {
                                    item[key] = item[key] || null
                                }
                            return item;
                        }
                        null;
                    });
                }
                if (errors.length > 0) {
                    return false;
                }
                var fields = inputValues['bbg_transport_CreateRequestField_request_id'].map(function (o) {
                    return o['field_name'];
                });
                var options = requestOptions.map(function (o) {
                    return {
                        'option_name': o['option_name'],
                        'option_value': o['option_value'],
                    };
                });
                var data = {
                    'request_description': inputValues['request_description'],
                    'requestor_code': inputValues['requestor_code'],
                    'program_code': programCode,
                    'interface_code': inputValues['interface_code'],
                    'response_format_code': inputValues['response_format_code'],
                    //'callback_uri': inputValues['callback_uri'],
                    'request_fields': fields,
                    'request_options': options,
                    'request_data_items': dataItems
                };
                _this.ajaxPost(Form.serviceBaseUrl + '/request_data', data);
                return true;
            };
            switch (_this.actionName) {
                case 'checkrequeststatus':
                    _this.actionToExecute = _this.actionCheckRequestStatus;
                    break;
                case 'createrequest':
                    _this.actionToExecute = _this.actionCreateRequest;
                    break;
            }
            return _this;
        }
        Form.servicePath = '/workshop/service/bbg_transport';
//      For local testing use
//      Form.serviceBaseUrl = 'http://ptp-dev' + Form.servicePath;
        Form.serviceBaseUrl = location.origin + Form.servicePath;
        return Form;
    }(Django.AdminForm));
    bbg_transport.Form = Form;
})(bbg_transport || (bbg_transport = {}));
var bbg_transport_Form = null;
function form_submit(el) {
    if (!bbg_transport_Form) {
        bbg_transport_Form = new bbg_transport.Form(el);
    }
    bbg_transport_Form.submit();
    $('a[href="#http_status"]').trigger('click');
}
