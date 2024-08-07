import json
import os.path
import time

import numpy as np
import pandas
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient import discovery
import gspread
import pandas as pd
from email.mime.text import MIMEText
import base64
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from Classes.InnerException import InnerException

from Classes.constants import DATA_LIBRARY_ID


# https://docs.gspread.org/en/v5.7.2/api/models/worksheet.html#id1


class GoogleAuth:
    REQUEST_DELAY = 1.0
    # If modifying these scopes, delete the file token.json.
    SCOPES = [
        'https://www.googleapis.com/auth/drive',
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/gmail.modify',
    ]

    def __init__(self):
        self.creds = self.authenticate()
        # Use the credentials to authenticate with gspread
        self.gc = gspread.Client(auth=self.creds)
        self.sheets = self.build_sheets()

    def build_sheets(self):
        """Builds a service that can interact with the Google Sheets API directly."""
        return discovery.build('sheets', 'v4', credentials=self.creds)

    def authenticate(self):
        """Authenticate the user and return the credentials."""
        creds = None
        # The file token.json stores the user's access and refresh tokens, and is
        # created automatically when the authorization flow completes for the first
        # time.
        if os.path.exists('keys/token.json'):
            creds = Credentials.from_authorized_user_file('keys/token.json', self.SCOPES)
        # If there are no (valid) credentials available, let the user log in.
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    'keys/data-manager-desktop.json', self.SCOPES)
                creds = flow.run_local_server(port=0)
            # Save the credentials for the next run
            with open('keys/token.json', 'w') as token:
                token.write(creds.to_json())
        return creds

    def authorize_request(self, workbook, library_item):
        """
        TODO: library_item has "job_class" which we could match up with the user table if we wanted to
        """
        if "user" not in workbook:
            print("no user in workbook")
            print(json.dumps(workbook))
            return False

        sheet = self.gc.open_by_key(DATA_LIBRARY_ID).worksheet("Users")
        users = sheet.get_all_records()
        for user in users:
            print(f"{workbook['user']}, {user['user']}, {user['allow_update']}")
            if workbook['user'] == user['user'] and user["allow_update"] == 'TRUE':
                return True
        return False

    def get_spreadsheet_tab(self, spreadsheet_id, tab_name) -> gspread.Worksheet:
        """Get a spreadsheet tab as a DataFrame."""
        time.sleep(self.REQUEST_DELAY)
        sheet = self.gc.open_by_key(spreadsheet_id).worksheet(tab_name)
        return sheet

    def get_data_from_tab(self, sheet: gspread.Worksheet) -> pd.DataFrame:
        time.sleep(self.REQUEST_DELAY)
        records = sheet.get_all_records()
        return pd.DataFrame(records)

    def get_spreadsheet_tab_data(self, spreadsheet_id, tab_name) -> pd.DataFrame:
        """Get a spreadsheet tab as a DataFrame."""
        time.sleep(self.REQUEST_DELAY)
        sheet = self.get_spreadsheet_tab(spreadsheet_id=spreadsheet_id, tab_name=tab_name)
        records = sheet.get_all_records()
        return pd.DataFrame(records)

    def get_workbook_title(self, spreadsheet_id):
        """Get the title of a spreadsheet (workbook)."""
        time.sleep(self.REQUEST_DELAY)
        workbook = self.gc.open_by_key(spreadsheet_id)
        title = workbook.title
        return title

    def update_status(self, spreadsheet_id, status_update_tab, status_update_range, message):
        """
        Update a specific range in the spreadsheet with the given message.

        Parameters:
            - spreadsheet_id (str): The ID of the spreadsheet.
            - range_name (str): The range to update in A1 notation (e.g., "Sheet1!A1").
            - message (str): The message to write in the specified range.

        Returns:
            None
        """
        time.sleep(self.REQUEST_DELAY)
        sheet = self.gc.open_by_key(spreadsheet_id).worksheet(
            status_update_tab)  # Replace 'sheet1' with the actual sheet name
        sheet.update(status_update_range, [[message]])

        return None

    def write_spreadsheet_tab(self, ssid: str, tab_name: str, data_df: pandas.DataFrame, options=None):
        """
        Write a DataFrame to a spreadsheet tab with specified options.

        Parameters:
            - spreadsheet_url (str): The URL of the spreadsheet.
            - tab_name (str): The name of the tab to write the DataFrame to.
            - data_frame (pd.DataFrame): The DataFrame to write.
            - options (dict, optional): Additional options for writing the spreadsheet.
                - clear (str): Clearing option. Possible values:
                    - 'columns': Clear only the columns in the tab before writing.
                    - 'all': Clear the entire tab before writing.
                    - None (default): Do not clear anything (default behavior).
                - copy_formulas (bool): Whether to copy formulas down in adjacent columns.
                    If True, formulas from the last row will be copied to newly written rows.

        Returns:
            None
        """
        self.clean_dataframe(data_df, "date")
        time.sleep(self.REQUEST_DELAY)
        spreadsheet = self.gc.open_by_key(ssid)
        # add new tab if tab is missing
        try:
            sheet = spreadsheet.worksheet(tab_name)
        except gspread.exceptions.WorksheetNotFound:
            # create a new worksheet
            time.sleep(self.REQUEST_DELAY)
            sheet = spreadsheet.add_worksheet(title=tab_name, rows="100", cols="26")
        # prep
        # data_frame = self.clean_dataframe(data_frame, "date")
        # Clear options
        clear_option = options.get("clear") if options else "columns"
        if clear_option == "columns":
            self.clear_columns(data_df, sheet)
        elif clear_option == "all":
            self.clear_all(data_df, sheet)
        # Write the DataFrame to the spreadsheet
        time.sleep(self.REQUEST_DELAY)
        values = data_df.values.tolist()
        columns = [data_df.columns.values.tolist()]
        sheet.update( columns + values, value_input_option="USER_ENTERED")
        if clear_option == "columns":
            self.copy_formulas_down(data_df, sheet)
        return None

        # New method signature as per warning
        # sheet.update(value=[data_frame.columns.values.tolist()] + data_frame.values.tolist(), range_name='your_range_name')

    def append_data_to_tab(self, spreadsheet_id: str, tab_name: str, data: pandas.DataFrame, options=None):
        # Google Sheet API setup
        self.clean_dataframe(data, "datetime")
        time.sleep(self.REQUEST_DELAY)
        spreadsheet = self.gc.open_by_key(spreadsheet_id)

        # Check if the tab exists, if not create a new one
        try:
            sheet = spreadsheet.worksheet(tab_name)
        except gspread.exceptions.WorksheetNotFound:
            time.sleep(self.REQUEST_DELAY)
            sheet = spreadsheet.add_worksheet(title=tab_name, rows="100", cols="26")

        # Get the number of existing rows
        time.sleep(self.REQUEST_DELAY)
        existing_rows = len(sheet.get_all_values())

        if existing_rows == 0:
            # If the sheet is empty, write data including headers
            sheet.update([data.columns.values.tolist()] + data.values.tolist(), value_input_option='USER_ENTERED')
        else:
            # If the sheet is not empty, write only data rows (exclude headers)
            sheet.append_rows(data.values.tolist(), value_input_option='USER_ENTERED',
                              insert_data_option='INSERT_ROWS', table_range=f"A{existing_rows + 1}")

    def clear_all(self, data, sheet):
        num_rows = data.shape[0] + 1  # Account for header row
        num_columns = data.shape[1]
        time.sleep(self.REQUEST_DELAY)
        sheet.resize(rows=num_rows, cols=num_columns)

    def clear_columns(self, data_frame, sheet):
        num_columns = data_frame.shape[1]
        num_rows = sheet.row_count
        df_rows = data_frame.shape[0] + 1  # Account for header row
        # If the sheet has more rows than the dataframe, delete the extra rows
        if num_rows > df_rows:
            sheet.delete_rows(df_rows + 1, num_rows)
        start_range = gspread.utils.rowcol_to_a1(1, 1)
        end_range = gspread.utils.rowcol_to_a1(num_rows, num_columns)
        time.sleep(self.REQUEST_DELAY)
        sheet.batch_clear([f"{start_range}:{end_range}"])

    def copy_formulas_down(self, df: pd.DataFrame, sheet: gspread.Worksheet):
        # Only copy formulas if there are extra columns in the sheet
        if sheet.col_count <= df.shape[1]:
            return
        column_count = df.shape[1]
        r2 = sheet.row_count
        # Define the source and destination ranges in A1 notation
        c1 = column_count + 1
        c2 = sheet.col_count
        source_range = f"{gspread.utils.rowcol_to_a1(2, c1)}:{gspread.utils.rowcol_to_a1(2, c2)}"
        dest_range = f"{gspread.utils.rowcol_to_a1(3, c1)}:{gspread.utils.rowcol_to_a1(r2, c2)}"
        # Copy the range
        time.sleep(self.REQUEST_DELAY)
        try:
            sheet.copy_range(source_range, dest_range, paste_type='PASTE_NORMAL')
        except gspread.exceptions.APIError as error:
            raise InnerException(error, "copy_formulas_down", {"df_shape": df.shape, "sheet_col_count": sheet.col_count,
                                                               "source_range": source_range, "dest_range": dest_range})
        except (gspread.exceptions.CellNotFound, gspread.exceptions.IncorrectCellLabel) as error:
            raise InnerException(error, "copy_formulas_down", {"df_shape": df.shape, "sheet_col_count": sheet.col_count,
                                                               "source_range": source_range, "dest_range": dest_range})

    def get_formulas_from_range(self, sheet, start_col: int, row1: int, end_col: int, row2: int):
        id = sheet.spreadsheet.id
        tab = sheet.title
        tab = "STU"
        # Convert column numbers to letters
        c1 = gspread.utils.col_to_a1(start_col)
        c2 = gspread.utils.col_to_a1(end_col)
        # Build the A1 notation range string
        range_ = f"{tab}!{c1}{row1}:{c2}{row2}"
        # Fetch the formulas
        request = self.sheets.spreadsheets().values().get(
            spreadsheetId=id,
            range=range_,
            valueRenderOption='FORMULA'
        )
        response = request.execute()
        return response.get('values', [[]])[0]

    def clean_dataframe(self, data, timestamp_conversion):
        class CustomJSONEncoder(json.JSONEncoder):
            def default(self, obj):
                if isinstance(obj, float) and np.isnan(obj):
                    return ""  # Replace nan with an empty string
                return json.JSONEncoder.default(self, obj)

        data = data.copy()

        for column in data.columns:
            data[column] = data[column].apply(
                lambda x: json.dumps(x, cls=CustomJSONEncoder) if isinstance(x, dict) else x
            )
        # for column in data.columns:
        #     # Apply the custom JSON encoder to your DataFrame column(s)
        #     data[column] = data[column].apply(lambda x: json.dumps(x) if isinstance(x, dict) else x)
        for col in data.select_dtypes(include=[np.datetime64, "datetime"]).columns:
            data[col] = data[col].astype(str)
        for col in data.select_dtypes(include=['float64']).columns:
            data[col].fillna(0, inplace=True)
        for col in data.select_dtypes(include=['object']).columns:
            data[col].fillna("", inplace=True)
        data.fillna("", inplace=True)

        columns = data.columns
        idx = 0
        for type_ in data.dtypes:
            if type_ == "datetime64[ns]":
                if timestamp_conversion == "date":
                    data[columns[idx]] = [dt.strftime('%Y-%m-%d') for dt in data[columns[idx]]]
                else:
                    data[columns[idx]] = [dt.strftime('%Y-%m-%d %H:%M') for dt in data[columns[idx]]]
            idx += 1
        data = data.astype(str)
        data.replace(r'\n', '', regex=True, inplace=True)
        data = data.replace({np.nan: None})
        return data

    def send_email(self, to, from_, subject, body):
        """Send an email using Gmail API."""

        service = build('gmail', 'v1', credentials=self.creds)
        message = self.create_message(to, from_, subject, body)
        self.send_message(service, 'me', message)

    def create_message(self, to, from_, subject, body):
        """Create an email message."""
        message = MIMEText(body)
        message['to'] = to
        message['from'] = from_
        message['subject'] = subject
        return {'raw': base64.urlsafe_b64encode(message.as_bytes()).decode()}

    def send_message(self, service, user_id, message):
        """Send an email message using the Gmail API."""
        try:
            message = service.users().messages().send(userId=user_id, body=message).execute()
            print(f"Message sent. Message ID: {message['id']}")
        except HttpError as ex:
            local = {"cause": "HttpError",
                     "user_id": user_id,
                     "message": message}
            raise InnerException(ex, self.send_message.__name__, local)


if __name__ == '__main__':
    auth = GoogleAuth()
    print('works')
