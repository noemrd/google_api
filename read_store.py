#!/usr/bin/env python3
"""
This module connects to the google API to read, store and reset columns C-G in the
'template1' tab in the template sheet
https://docs.google.com/spreadsheets/d/xxxxxxx/
edit#gid=12345
"""

import gsheets_utils
import sys
import sqlalchemy
import config
from config import PSQL_STRING
from gsheets_utils import set_non_int
from sqlalchemy import create_engine, Column, Integer, String


class COLS:
    """
    WARNING: These constants must match the zero-based column indexes in the spreadsheet.
    If they don't, then the wrong data will be harvested and inserted into the database.
    """
    ID = 0
    VALUE_1 = 2
    VALUE_2 = 3
    VALUE_3 = 4
    VALUE_4 = 5
    VALUE_5 = 6


def set_up(values):
    """
    This function places the values in dicts for the database upload
    """
    all_rows = []
    for x in values:
        new_row = {"id": x[COLS.ID],
                   "value_1": set_non_int(x[COLS.VALUE_1]),
                   "value_2": set_non_int(x[COLS.VALUE_2]),
                   "value_3": set_non_int(x[COLS.VALUE_3]),
                   "value_4": set_non_int(x[COLS.VALUE_4]),
                   "value_5": set_non_int(x[COLS.VALUE_5])}
        all_rows.append(new_row)
    return all_rows


def insert_info(all_rows):
    """
    This function is used to insert all rows in the database
    """
    engine = create_engine(PSQL_STRING,
                           convert_unicode=True)
    metadata = sqlalchemy.MetaData()
    table = sqlalchemy.Table('warehouse_inventory_history', metadata,
                             Column('id', Integer),
                             Column('value_1', Integer),
                             Column('value_2', Integer),
                             Column('value_3', Integer),
                             Column('value_4', Integer),
                             Column('value_5', String),
                             schema='beta'
                             )

    ins = table.insert().values(all_rows)
    engine.execute(ins)


def main():
    template_sheet = gsheets_utils.set_up_sheet(config.CLIENT_SECRET_PATH,
                                                           'template1',
                                                           'xxxxxxx')
    values = gsheets_utils.fetch_values(template_sheet, 'A2:G')
    all_rows = set_up(values)
    insert_info(all_rows)
    gsheets_utils.set_values_zero(template_sheet, 'C2:G')


if __name__ == "__main__":
    sys.exit(main())
