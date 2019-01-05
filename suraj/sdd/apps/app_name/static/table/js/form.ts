 interface jQuery { }
module Django {
    export abstract class AdminForm {
        protected actionName: string;
        protected actionToExecute: (inputValues: Object, errors: Array<string>) => boolean;
        protected form: any;

        constructor(el: Element) { }

        protected ajaxGet = (url: string): jQuery => { return null; };
        protected ajaxPost = (url: string, data: {} | FormData): jQuery => { return null; }
        protected onAjaxComplete = (): void => { }
        protected onAjaxError = (xhr, status, errorThrown): void => { }
        protected onAjaxSuccess = (data, status, xhr): void => { }
        protected printHttpRequest = (settings: any): void => { }
        protected printHttpStatus = (status: string, message?: any): void => { }
        protected toggleFormDisable = (disable: boolean): void => { }
        protected toFormData = (file: File, expectedExtension: string): FormData => { return null; }
        protected toPrismHtml = (data: any): string => { return null; }

        public submit(): void { }
    }
}
/* DELETE FROM HERE AND UP FROM THE GENERATED CODE */
/* =============================================== */

module bbg_transport {
    export class Form extends Django.AdminForm {
        static servicePath: string = '/workshop/service/bbg_transport';
        static serviceBaseUrl: string = location.origin + Form.servicePath;
        constructor(el: Element) {
            super(el);

            switch (this.actionName) {
                case 'checkrequeststatus':
                    this.actionToExecute = this.actionCheckRequestStatus;
                    break;
                case 'createrequest':
                    this.actionToExecute = this.actionCreateRequest;
                    break;
            }
        }

        private actionCheckRequestStatus = (inputValues: Object, errors: Array<string>): boolean => {
            var action = inputValues['action'];
            switch (action) {
                case 'check_status':
                case 'response':
                    break;
                default:
                    return false;
            }

            var requestId = inputValues['request_id'];
            this.ajaxGet(Form.serviceBaseUrl + '/' + action + '/' + requestId);
            return true;
        }

        private actionCreateRequest = (inputValues: Object, errors: Array<string>): boolean => {
            var programCode = inputValues['program_code'];

            var requestOptions: {}[] = inputValues['bbg_transport_CreateRequestOption_request_id'];
            if (programCode === 'GETHISTORY' && requestOptions.length === 0) {
                errors.push('GETHISTORY requires least 1 Request Option.');
            }

            var requestDataItems: {}[] = inputValues['bbg_transport_CreateRequestDataItem_request_id'];
            if (requestDataItems.length === 0) {
                errors.push('At least 1 Request Data Item is required.');
            } else {
                var dataItems = inputValues['bbg_transport_CreateRequestDataItem_request_id'].map(o => {
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
                    } else if (item.yellow_key === '' && item.ticker !== '') {
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

            var fields = inputValues['bbg_transport_CreateRequestField_request_id'].map(o => {
                return o['field_name'];
            });

            var options = requestOptions.map(o => {
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

            this.ajaxPost(Form.serviceBaseUrl + '/request_data', data);

            return true;
        }
    }
}
var bbg_transport_Form: bbg_transport.Form = null;

function form_submit(el: Element): void {
    if (!bbg_transport_Form) {
        bbg_transport_Form = new bbg_transport.Form(el);
    }

    bbg_transport_Form.submit();
}
