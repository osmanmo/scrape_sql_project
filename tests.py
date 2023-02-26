import os
import unittest
import zipfile
from unittest.mock import patch, MagicMock
import main
import pandas as pd
from sqlalchemy import create_engine

base_url = 'https://www.stats.govt.nz/large-datasets/csv-files-for-download'


class TestMain(unittest.TestCase):
    def test_unzip_file(self):
        # Create fake files for the test
        with open('file1.txt', 'w') as f:
            f.write('This is file1.txt')

        os.mkdir('folder1')
        with open('folder1/file2.txt', 'w') as f:
            f.write('This is file2.txt')
        with open('folder1/file3.txt', 'w') as f:
            f.write('This is file3.txt')

        # Create a zip file with the fake files
        with zipfile.ZipFile('test.zip', mode='w') as zip_file:
            zip_file.write('file1.txt')
            zip_file.write('folder1/file2.txt')
            zip_file.write('folder1/file3.txt')

        # Call the unzip_file function to extract the files
        extracted_files = main.unzip_file('test.zip')

        # Check that the extracted files are correct
        expected_files = ['file1.txt', 'folder1/file2.txt', 'folder1/file3.txt']
        assert extracted_files == expected_files, f"Expected {expected_files}, but got {extracted_files}"

        # Remove the fake files
        os.remove('file1.txt')
        os.remove('folder1/file2.txt')
        os.remove('folder1/file3.txt')
        os.rmdir('folder1')
        os.remove('test.zip')

    def test_create_table(self):
        # Create a test DataFrame
        data = {'col1': [1, 2, 3], 'col2': ['a', 'b', 'c']}
        df = pd.DataFrame(data)

        # Create an in-memory SQLite database engine for testing
        engine = create_engine('sqlite:///:memory:')

        # Call the create_table function to create a new table in the database
        file_name = 'test-file.csv'
        main.create_table(df, file_name, engine)

        # Check that the table was created and contains the correct data
        result = engine.execute('SELECT * FROM test_file')
        rows = result.fetchall()
        expected_rows = [(1, 'a'), (2, 'b'), (3, 'c')]
        assert rows == expected_rows, f"Expected {expected_rows}, but got {rows}"


if __name__ == '__main__':
    unittest.main()
