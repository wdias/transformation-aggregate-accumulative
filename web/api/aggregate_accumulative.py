import requests
from flask import Blueprint, request, jsonify, json

from web import util

bp = Blueprint('aggregate_accumulative', __name__)
callback_url = f'{util.HOST_URL}/extension/transformation/aggregate-accumulative'
adapter_status = 'http://adapter-status.default.svc.cluster.local'

""" Transformation Structure:
{
    extensionId: "",
    extension: enum("Transformation", "Validation", "Interpolation"),
    function: "AggregateAccumulative",
    inputVariables: [
        {
            variableId: "",
            timeseriesId: "",
            data: [{
                time: "",
                value: ""
            }]
        }
    ],
    outputVariables: [
        {
            variableId: "",
            timeseriesId: ""
        }
    ],
    options: {},
    callback: "",

    start: "",
    end: "",
    token: "",
}
"""


@bp.route('/extension/transformation/aggregate-accumulative', methods=['POST'])
def extension_transformation_aggregate_accumulative():
    extension = request.get_json()
    print("Extension Transformation AggregateAccumulative:", extension)
    assert 'extensionId' in extension, f'extensionId should be provided'
    assert 'inputVariables' in extension and isinstance(extension['inputVariables'], list), \
        f'inputVariables should be provided'
    assert 'outputVariables' in extension and isinstance(extension['outputVariables'], list), \
        f'outputVariables should be provided'

    trigger_data = {
        "output_variables": extension['outputVariables'],
        "input_variables": extension['inputVariables'],
        "options": extension['options'],
        'callback': extension['callback'],
        "token": request.args.get('token'),
        "requestId": request.args.get('requestId'),
        "start": request.args.get('from'),
        "end": request.args.get('end'),
    }
    output_variables = process_aggregate_accumulative(**trigger_data)
    trigger_callback(output_variables, trigger_data.get('callback'), trigger_data.get('token'))
    if request.args.get('requestId'):
        for output_var in output_variables:
            print('1111 ', output_var, trigger_data.get('requestId'))
            timeseries_Id = output_var.get('timeseriesId')
            set_request_status(timeseries_Id, trigger_data.get('requestId'), "Extension", "Transformation", "aggregate-accumulative")
    else:
        print("WARN: requestId not found. Skip status update.")

    del extension['inputVariables']
    del extension['outputVariables']
    extension['callback'] = f'{util.HOST_URL}/extension/transformation/aggregate-accumulative',
    return jsonify(extension)


def process_aggregate_accumulative(input_variables=None, output_variables=None, options=None, **kwargs):
    if options is None:
        options = {}
    if output_variables is None:
        output_variables = []
    if input_variables is None:
        input_variables = []
    input_x = input_variables[0]
    output_y = output_variables[0]
    output_y['data'] = input_x['data']
    return [output_y]


def trigger_callback(output_variables, callback, token):
    print("Trigger callback: ", output_variables, callback, token)
    callback_trigger = {
        "outputVariables": output_variables,
        "callback": callback_url,
    }
    res = requests.post(f'{callback}/{token}', data=callback_trigger)
    print(res.status_code, res.text)
    assert res.status_code is 200, f'Unable to update job completion token: {token} for {callback}'
    return

def set_request_status(timeseriesId, requestId, service, type, extensionFunction):
    print("Set status", timeseriesId, requestId, service, type, extensionFunction)
    req_status = {
        "requestId": requestId,
        "service": service,
        "type": type,
        "extensionFunction": extensionFunction,
    }
    res = requests.post(f'{adapter_status}/{timeseriesId}', data=req_status)
    assert res.status_code is 200, f'Unable to set request status: {requestId} for {extensionFunction}'
    return
