""" Module for download data from service hosted or arcgis server
    by nicogis
"""
import sys
import os.path
import traceback
import inspect
from urllib.parse import urlencode
import shutil
import requests
import arcpy

# Here you can set a proxy
PROXIES = {}
# For example:
# PROXIES = {
#  "http": "http://10.10.1.10:3128",
#  "https": "http://10.10.1.10:1080",
# }

# Here you can set th CHUNK
CHUNK = 100

def trace():
    """Determines information about where an error was thrown.
    Returns:
        tuple: line number, filename, error message
    Examples:
        >>> try:
        ...     1/0
        ... except:
        ...     print("Error on '{}'\\nin file '{}'\\nwith error '{}'".format(*trace()))
        ...
        Error on 'line 1234'
        in file 'C:\\foo\\baz.py'
        with error 'ZeroDivisionError: integer division or modulo by zero'
    """
    tbk = sys.exc_info()[2]
    tbinfo = traceback.format_tb(tbk)[0]
    filename = inspect.getfile(inspect.currentframe())
    # script name + line number
    line = tbinfo.split(', ')[1]
    # Get Python syntax error
    synerror = traceback.format_exc().splitlines()[-1]
    return line, filename, synerror

def is_blank(value):
    """check string is None OR is empty or blank
    Arguments: string tested
    """
    return not (value and value.strip())

def chunklist(values, chunk):
    """Yield successive chunk-sized chunks from values.

    Args:
        values (object): The object to chunk.
        chunk (int): The size of the chunks.
    Yields:
        The next chunk in the object.
    Raises:
        TypeError: if ``l`` has no :py:func:`len`.
    Examples:
        >>> for c in chunklist(list(range(20)), 6):
        ...     print(c)
        [0, 1, 2, 3, 4, 5]
        [6, 7, 8, 9, 10, 11]
        [12, 13, 14, 15, 16, 17]
        [18, 19]
        >>> list(chunklist(string.ascii_uppercase, 7))
        ['ABCDEFG', 'HIJKLMN', 'OPQRSTU', 'VWXYZ']

    """
    chunk = max(1, chunk)
    for i in range(0, len(values), chunk):
        yield values[i:i+chunk]

def init_params(token):
    """initialize params"""
    params = {'f' :'json'}
    if not token is None:
        params['token'] = token

    return params

def get_max_record_count(url, token):
    """num record max returned from query"""
    response = CHUNK
    try:
        params = init_params(token)
        max_record_count = request(url, params)['maxRecordCount']
        if CHUNK > max_record_count:
            return max_record_count
    except:
        pass

    return response

def get_record_count(url, params):
    """number records of query"""
    response = None
    try:
        params['returnCountOnly'] = str(True).lower()
        response = request(url, params)['count']
    except:
        pass

    return response

def supports_pagination(url, token):
    """layer support pagination"""
    response = False
    try:
        params = init_params(token)
        response = request(url, params)['advancedQueryCapabilities']['supportsPagination']
    except:
        pass

    return response

def get_has_attachments(url, token):
    """the service has Attachments"""
    has_attachments = False
    try:
        params = init_params(token)
        has_attachments = request(url, params)['hasAttachments']
    except:
        arcpy.AddWarning('Problem get hasAttachments info in service')

    return has_attachments

def request(url, params):
    """request post"""
    response = None
    try:
        with requests.post(url, data=params, proxies=PROXIES) as req:
            response = req.json()
        if 'error' in response:
            raise Exception(response['error']['message'])
    except requests.exceptions.Timeout:
        raise Exception(f'Connection to {url} timed out')
    except requests.exceptions.ConnectionError:
        raise Exception(f'Unable to connect to host at {url}')
    except requests.exceptions.URLRequired:
        raise Exception(f'Invalid URL - {url}')
    except:
        raise
    return response

def records_desc(table):
    """description records"""
    return 'Rows' if table else 'Features'

def is_table(base_url, token):
    """check if service layer is a table"""
    params = init_params(token)
    data = request(base_url.rstrip('/'), params)
    type_layer = data['type']
    return type_layer.lower() == 'table'

def download_file(url, local_filename):
    """download file from url in stream"""
    try:
        with requests.get(url, stream=True, proxies=PROXIES) as req:
            if req.status_code == 200:
                with open(local_filename, 'wb') as file:
                    req.raw.decode_content = True
                    shutil.copyfileobj(req.raw, file)
        return local_filename
    except:
        arcpy.AddWarning(f'Error or file not found: {url}')
        return None

def get_params_query(where_clause, spatial_filter, spatial_relationship, token):
    """build query params"""
    params = init_params(token)

    # set where
    if is_blank(where_clause):
        params['where'] = '1=1'
    else:
        params['where'] = where_clause

    #set spatial filter
    desc_fset = arcpy.Describe(spatial_filter)
    if desc_fset.file:
        shape_json = None
        with arcpy.da.SearchCursor(spatial_filter, ["SHAPE@"]) as cur:
            for shape, in cur:
                if shape_json:
                    shape_json = shape_json.union(shape)
                else:
                    shape_json = shape
        shape_json = shape_json.JSON
        params['geometryType'] = f'''esriGeometry{desc_fset.shapeType}'''
        params['geometry'] = shape_json
        params['spatialRel'] = spatial_relationship
        params['inSR'] = desc_fset.spatialReference.factoryCode

    return params

def get_object_ids(url, params):
    """list oids"""
    response = None
    try:
        params['returnIdsOnly'] = str(True).lower()
        response = request(url, params)['objectIds']
    except:
        pass

    return response

def get_oids(base_url, where_clause, spatial_filter, spatial_relationship, token):
    """list of oids"""
    params_query = get_params_query(where_clause, spatial_filter, spatial_relationship, token)
    num_recs = get_record_count(add_url_path(base_url, 'query'), params_query.copy())

    oids = []
    if supports_pagination(base_url, token) and not num_recs is None:
        if num_recs > 0:
            max_record_count = get_max_record_count(base_url, token)
            iterations = num_recs // max_record_count + 1
            if (num_recs % max_record_count) == 0:
                iterations -= 1

            params_query['resultRecordCount'] = max_record_count
            for chunk in range(0, iterations):
                offset = chunk * max_record_count
                params_query['resultOffset'] = offset
                oids += get_object_ids(add_url_path(base_url, 'query'), params_query)
    else:
        # return only oids
        oids = get_object_ids(add_url_path(base_url, 'query'), params_query)

    return oids

def add_url_path(base_url, *args):
    """add path url"""
    base_url = base_url.rstrip('/')
    for arg in args:
        base_url += '/' + str(arg).strip('/')
    return base_url

def generate_token(base_url, user, password):
    """generate token arcgis server"""
    server, instance = tuple(base_url.split('/')[2:4])
    base = f'https://{server}'
    token_url = add_url_path(base, instance, 'tokens', 'generateToken')
    params = {'username' : user, 'password' : password, 'client' : 'requestip', 'expiration' : 60, 'f' :'json'}
    response = request(token_url, params)
    return response['token']

def get_token(base_url):
    """get token"""
    token = None
    try:
        hosted_feature_service = arcpy.GetParameter(0)
        ags_service = arcpy.GetParameter(1)
        portal_url = arcpy.GetParameterAsText(2)
        username = arcpy.GetParameterAsText(7)
        password = arcpy.GetParameterAsText(8)
        if not (is_blank(username) or is_blank(password)):
            arcpy.AddMessage('\nGenerating Token\n')
            # Generate token for hosted feature service
            if hosted_feature_service:
                arcpy.SignInToPortal(portal_url, username, password)
                token = arcpy.GetSigninToken()['token']
            # Generate token for AGS feature service
            elif ags_service:
                token = generate_token(base_url, username, password)
            if token is None:
                raise ValueError('Error generate token')
    except:
        raise Exception('Error generate token')
    return token

def download_data(base_url, token, oids, is_layer_table, chunk):
    """download data"""
    params = init_params(token)
    params['outFields'] = '*'

    total_downloaded = 0
    featuresets = []
    total = len(oids)
    chunk_size = min([chunk, total])
    describe_recs = records_desc(is_layer_table)
    arcpy.ResetProgressor()
    arcpy.SetProgressor('step', f'''{total} {describe_recs.lower()} to be downloaded''', 0, total, chunk)
    url = add_url_path(base_url, 'query')
    for current_chunk in chunklist(oids, chunk_size):
        oids_query = ",".join(map(str, current_chunk))
        if not oids_query:
            continue
        else:
            featureset = arcpy.RecordSet() if is_layer_table else arcpy.FeatureSet()
            params['objectIds'] = oids_query
            try:
                featureset.load(f'''{url}?{urlencode(params)}''')
            except:
                arcpy.AddError('Try to set a lower value for variable CHUNK')
                raise

            featuresets.append(featureset)
            total_downloaded += chunk_size
            arcpy.SetProgressorLabel(f'''{total_downloaded} {describe_recs.lower()} appended''')
            arcpy.SetProgressorPosition()
    return featuresets

def download_attachments(folder_attachments, base_url, token, oids, is_layer_table):
    """download attachments"""
    if not is_blank(folder_attachments):
        has_attachments = get_has_attachments(base_url, token)
        if has_attachments:
            arcpy.AddMessage('Please wait a moment ... download attachments ...')
            params = init_params(token)
            total = len(oids)
            describe_recs = records_desc(is_layer_table)
            arcpy.ResetProgressor()
            arcpy.SetProgressor('step', f'''{total} {describe_recs.lower()} to be downloaded''', 0, total)
            total_progress = 0

            for oid in oids:
                url_attachment = add_url_path(base_url, oid, 'attachments')
                attachment_infos = request(url_attachment, params)['attachmentInfos']
                if attachment_infos:
                    for attachment_info in attachment_infos:
                        id_attachment = attachment_info['id']
                        id_attachment_name = attachment_info['name']
                        name_file = f'{oid}-{id_attachment}-{id_attachment_name}'
                        download_file(f'''{add_url_path(url_attachment, id_attachment)}?{urlencode(params)}''', os.path.join(folder_attachments, name_file))
                total_progress += 1
                arcpy.SetProgressorLabel(f'''{total_progress} {describe_recs.lower()} attachments''')
                arcpy.SetProgressorPosition()
        else:
            arcpy.AddWarning('Service hasn\'t attachments')

def download_service():
    """download service"""
    try:
        base_url = arcpy.GetParameterAsText(3)
        where_clause = arcpy.GetParameterAsText(4)
        spatial_filter = arcpy.GetParameter(5)
        spatial_relationship = arcpy.GetParameterAsText(6)
        folder_attachments = arcpy.GetParameterAsText(9)
        output_fc = arcpy.GetParameterAsText(10)
        token = get_token(base_url)
        is_layer_table = is_table(base_url, token)
        chunk = get_max_record_count(base_url, token)
        arcpy.AddMessage(f'CHUNK used: {chunk}')
        describe_recs = records_desc(is_layer_table)
        arcpy.AddMessage('Please wait a moment ... loading oids ...')
        oids = get_oids(base_url, where_clause, spatial_filter, spatial_relationship, token)

        if oids:
            # Download data
            featuresets = download_data(base_url, token, oids, is_layer_table, chunk)
            arcpy.Merge_management(featuresets, output_fc)

            # Download attachments
            download_attachments(folder_attachments, base_url, token, oids, is_layer_table)

        else:
            arcpy.AddWarning(f'{describe_recs} not found')
    except:
        arcpy.AddError('Error on \'{}\'\nin file \'{}\'\nwith error \'{}\''.format(*trace()))
    finally:
        arcpy.ResetProgressor()
download_service()
