import sys
import openpyxl
from io import BytesIO
from cdisutils.log import get_logger
from cdisutils.storage import BotoManager
from urlparse import urlparse

log = get_logger(
    "excel"
)

def combine_headers(headers):
    index = 0
    last_header = None
    new_header = []
    for value in headers[0]:
        if value:
            last_header = value
            if headers[1][index]:
                new_value = value + " " + headers[1][index]
            else:
                new_value = value
        else:
            if headers[1][index]:
                new_value = last_header + " " + headers[1][index]
            else:
                new_value = None
        if new_value:
            new_header.append(new_value)
        index += 1

    return new_header


def read_sheet(ws=None, num_headers=1):
    headers = []
    sheet_data = []
    cur_row = 1
    header_combine = False
    header = None
    if num_headers > 2:
        log.info("Sorry, can only process 2 or fewer header rows")
    else:
        if num_headers > 1:
            header_combine = True
        log.info("{} rows in sheet".format(ws.max_row))

        for row in ws.rows:
            if num_headers > 0:
                header_data = []
                for cell in row:
                    if cell.value:
                        header_data.append(cell.value.strip())
                    else:
                        header_data.append(None)
                headers.append(header_data)
                num_headers -= 1
                if num_headers == 0:
                    if header_combine:
                        header = combine_headers(headers)
                    else:
                        header = headers[0]
            else:
                # try and strip any blank rows
                any_data = False
                for cell in row:
                    if cell.value:
                        if len(cell.value.strip()):
                            any_data = True
                            break

                if any_data:
                    line_data = dict(zip(header, [cell.value for cell in row]))
                    sheet_data.append(line_data)
            cur_row += 1

    return sheet_data

def load_spreadsheet_from_s3(boto_man=None,
                             uri=None,
                             sheet_names=None,
                             num_headers=1):
    downloading = True
    total_transfer = 0
    data = None
    file_data = bytearray()
    chunk_size=16777216
    urlparts = urlparse(uri)
    s3_loc =  urlparts.netloc.split('.')[0]
    bucket = urlparts.path.split('/')[1]
    key_name = urlparts.path.split('/')[-1]
    


    # This is a bit tricky here. openpyxl likes a file, so we're
    # tricking it a bit by loading the data first, because we want to
    # stream from S3.
    data = boto_man.load_file(uri=uri)
    file_data.extend(data)  

    try:
        wb = openpyxl.load_workbook(filename=BytesIO(file_data))
    except Exception as e:
        print 'Hmm, something wrong: {}'.format(e)
        if key_name.endswith('xls'):
            print 'Loading as xlsx'
            wb = openpyxl.load_workbook(filename=BytesIO(file_data),
                                        data_only=False)
        else:
            print 'Loading as xls'
            wb = openpyxl.load_workbook(filename=BytesIO(file_data),
                                        data_only=True)
    else:
        pass
    ws = None
    data = {}
    for name in wb.sheetnames:
        for sheet_name in sheet_names:
            if sheet_name in name:
                log.info("Found sheet: %s" % name)
                ws = wb[name]
                data[sheet_name] = read_sheet(ws=ws,
                                              num_headers=num_headers)

    return data


