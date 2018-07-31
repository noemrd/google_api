#!/usr/bin/env python3
"""
This module connects to the google API to read or write to sheets
"""

import pygsheets
from sqlalchemy import create_engine
from config import PSQL_STRING
import simplejson
import datetime
import decimal
import re


def set_up_sheet(client_secret_file, title, spread_sheet_id):
    """
    This function is used to set up a sheet for reading or writing
    """
    gc = pygsheets.authorize(service_file=client_secret_file)
    spreadsheet = gc.open_by_key(spread_sheet_id)
    work_sheet = spreadsheet.worksheet_by_title(title)
    return work_sheet


def fetch_values(work_sheet, range_):
    """
    This function is used to read data from a sheet
    """
    start_range, end_range = range_.split(':')
    values = work_sheet.get_values(
        start=start_range,
        end='{}{}'.format(end_range, work_sheet.rows),
        include_empty=True
    )
    return values


def clear_sheet(work_sheet, range_):
    """
    This function is used to clear data from a sheet
    """
    exact_rangee = exact_range(range_, work_sheet.rows)
    start_range, end_range = exact_rangee.split(':')
    work_sheet.clear(
        start=start_range,
        end=end_range,
    )


def db_query(psql_string):
    """
    This function runs a query
    """
    engine = create_engine(PSQL_STRING,
                           convert_unicode=True,
                           connect_args={"options": "-c timezone=US/Pacific"})
    qry = engine.execute(psql_string)
    values = encoder(qry)
    num_rows = len(values)
    return values, num_rows


def post_values(work_sheet, range_, psql_string=None, new_values=None):
    """
    This function is used to post values provided or returned by a query.
    If values are provided, a row_count also needs to be provided
    """
    if new_values is not None:
        row_count = len(new_values)
        exact_rangee = exact_range(range_, row_count)
    else:
        new_values, num_rows = db_query(psql_string)
        exact_rangee = exact_range(range_, num_rows)
    work_sheet.update_cells(exact_rangee, new_values)


def set_values_zero(work_sheet, range_):
    """
    This function sets all values in range to 0
    """
    new_valuess = []
    values = fetch_values(work_sheet, range_)
    for x in values:
        value = []
        for y in x:
            value.append(0)
        new_valuess.append(value)
    post_values(work_sheet, range_, new_values=new_valuess)


def encoder(result):
    """
    This function takes the result of a query, then encodes decimals and datetime values.
    For example, datetime.date(2015, 11, 29) becomes '2015-11-29',
            and Decimal('200.08') becomes '200.08'
    The returned value is the result of a query with datetime and Decimal types converted to strings
    """
    all_rows = []
    for row in result.fetchall():
        new_row = []
        for x in row:
            if isinstance(x, decimal.Decimal):
                x = simplejson.dumps(x)
            if isinstance(x, datetime.date):
                x = x.__str__()
            new_row.append(x)
        all_rows.append(new_row)
    return all_rows


def exact_range(range_, num_rows):
    """
    Writing to a sheet requires an exact range, 'A4:B12' for example. Since we do not know the
    exact range ahead of time (as we only know 'A4:B'), this function is used to calculate the
    maximum value of a range. We get the maximum value by adding the number of rows to be pasted
    in the sheet to the minimum value -1.
    For example, if the range is 'A4:K', and there are 5 rows to be pasted in the sheet,
    the exact range will be 5 + 4 -1 = 8 -> 'A4:k8'
    """
    range_min = range_.split(':')[0]
    range_min_num = int(re.findall('\d+',range_min)[0])-1
    exact_rangee = '{}{}'.format(range_, num_rows + range_min_num)
    return exact_rangee


def set_non_int(val):
    """
    This function sets any non int, and any number less than 0, to 0
    """
    if val is None:
        val = 0
    try:
        val = int(val)
        if val < 0:
            val = 0
    except ValueError:
        val = 0
    return val
