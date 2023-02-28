from google.cloud import bigquery
import os
def main(request):
    """Responds to any HTTP request.
    Args:
        request (flask.Request): HTTP request object.
    Returns:
        The response text or any set of values that can be turned into a
        Response object using
        `make_response <http://flask.pocoo.org/docs/1.0/api/#flask.Flask.make_response>`.
    """
    client = bigquery.Client()
    job = client.query('SELECT COUNT(*) AS qtd FROM {}.{}.{}'.format(os.environ['project_id'], os.environ['dataset'], os.environ['table_name']))
    request_json = request.get_json()
    results = job.result()
    results_str = ''
    for row in results:
        results_str += "{}".format(row.qtd)
    if request.args and 'message' in request.args:
        return request.args.get('message')
    elif request_json and 'message' in request_json:
        return request_json['message']
    else:
        return results_str