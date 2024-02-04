import os
import sys
import sqlite3
import unittest
import tempfile
import pandas as pd
import pandas.api.types as ptypes

sys.path.append(os.path.abspath(os.path.join('../scripts')))
from load_data import LoadData

class TestLoadData(unittest.TestCase):
    def setUp(self):

        self.db_path = tempfile.mktemp()
        self.load_data = LoadData(self.db_path)
        self.load_data.connect()

    def tearDown(self):

        self.load_data.commit_and_close()
        os.remove(self.db_path)

    def test_sanitize_name(self):

        original_name = "My Table: Data"
        sanitized_name = self.load_data.sanitize_name(original_name)
        self.assertEqual(sanitized_name, "My_Table_Data")

    def test_create_table(self):

        table_name = "TestTable"
        columns = "column1 TEXT, column2 INTEGER"
        self.load_data.create_table(table_name, columns)

        # Check if the table is created
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(f"PRAGMA table_info({table_name})")
            table_info = cursor.fetchall()

        expected_columns = [("column1", "TEXT", 0, None, 0), ("column2", "INTEGER", 0, None, 0)]
        self.assertEqual(table_info, expected_columns)

if __name__ == '__main__':
    unittest.main()
