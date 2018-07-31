#!/usr/bin/env python3
"""
This module connects to the google API to update template2
https://docs.google.com/spreadsheets/d/xxxxxx/edit#gid=
12345
"""

import gsheets_utils
import sys
import config


class SheetInfo:
    """
    This class contains information to update column A-B in the 'template2' tab in
    https://docs.google.com/spreadsheets/d/xxxxxx/
    edit#gid=12345
    """
    spreadsheet_id = 'xxxxxx'
    title = 'template2'
    rangee = 'A2:B'
    psql_string = """
SELECT id, 
    title 
    FROM schema1.table1 
    """


def main():
    weekly_sheet = gsheets_utils.set_up_sheet(config.CLIENT_SECRET_PATH,
                                              SheetInfo.title,
                                              SheetInfo.spreadsheet_id)
    gsheets_utils.clear_sheet(weekly_sheet, SheetInfo.rangee)
    gsheets_utils.post_values(weekly_sheet, SheetInfo.rangee, SheetInfo.psql_string)
    

if __name__ == "__main__":
    sys.exit(main())
