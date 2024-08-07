

import xml.etree.ElementTree as ET
import pandas as pd
from Classes.GoogleAuth import GoogleAuth

class SamXmlReader:
    def __init__(self):
        pass

    def get_dataframe(self, path_xml_file: str) ->pd.DataFrame:
        # Parse the XML file
        tree = ET.parse(path_xml_file)
        root = tree.getroot()

        # Extract data into a list of dictionaries
        data = []
        for record in root.findall('.//RECORD'):
            record_data = {}
            for child in record:
                record_data[child.tag] = child.text
            data.append(record_data)

        # Convert the data into a DataFrame
        df = pd.DataFrame(data)
        return df


if __name__ == '__main__':
    """
    23/24 SRI Uploader v.2 XML
    ss_url = "https://docs.google.com/spreadsheets/d/11oXuJSDd73fum2zDSYahdxgDqAiyBxiQ0El3kpeHp6o/edit#gid=1905827910"
    """
    ssid = "11oXuJSDd73fum2zDSYahdxgDqAiyBxiQ0El3kpeHp6o"
    files = [
        'SamStudentsXml/data_slms_student_0.xml',
        'SamStudentsXml/data_slms_student_1.xml',
        'SamStudentsXml/data_slms_student_2.xml'
    ]
    reader = SamXmlReader()
    all_dfs = []  # List to store individual DataFrames

    for file in files:
        df = reader.get_dataframe(path_xml_file=file)
        all_dfs.append(df)

    # Concatenate all DataFrames
    tests_df = pd.concat(all_dfs, ignore_index=True)
    columns = ['SIS_ID', 'LEXILE_LEVEL', 'LEXILE_SCORE', 'LEXILE_MOD_DATE',
     'LEXILE_FULLY_COMPUTED', 'GRADE_ID', ]
    tests_df = tests_df[columns]
    tests_df = tests_df[(pd.notnull(tests_df['LEXILE_SCORE'])) & (tests_df['LEXILE_SCORE'] != '0')].copy()
    auth = GoogleAuth()
    options = {"clear": "all"}
    auth.write_spreadsheet_tab(ssid=ssid, data_df=tests_df, tab_name="SamExport", options=options)
