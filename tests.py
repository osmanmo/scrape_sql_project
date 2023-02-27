import os
import unittest
import zipfile
from unittest.mock import patch, Mock

import pandas as pd
from sqlalchemy import create_engine

import main

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

    @patch('main.requests.get')
    @patch('main.logger.info')
    def test_gets_urls_from_business_section(self, mock_logger_info, mock_requests_get):
        mock_response = """<div data-value='{"PageBlocks":[{"Title":"Business","BlockDocuments":
        [{"DocumentLink":"/article1"},{"DocumentLink":"/article2"}]}]}' id="pageViewData"></div>"""
        mock_requests_get.return_value = Mock(text=mock_response, spec_set=['status_code', 'text'])
        urls = main.get_list_of_urls_in_business_section('https://www.example.com')
        self.assertEqual(urls, ['https://www.example.com/article1', 'https://www.example.com/article2'])
        mock_logger_info.assert_called_with(['https://www.example.com/article1', 'https://www.example.com/article2'])
        mock_requests_get.assert_called_with('https://www.example.com')

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

    @patch('main.pd.read_csv')
    @patch('main.get_list_of_urls_in_business_section')
    @patch('main.get_file_name_from_url')
    @patch('main.download_file')
    @patch('main.unzip_file')
    @patch('main.create_table')
    @patch('main.sqlalchemy.create_engine')
    @patch('main.logger.info')
    def test_main(self, mock_logger_info, mock_create_engine, mock_create_table, mock_unzip_file, mock_download_file,
                  mock_get_file_name_from_url, mock_get_list_of_urls_in_business_section, mock_read_csv):
        mock_read_csv.return_value = pd.DataFrame({'col1': [1, 2], 'col2': [3, 4]})
        mock_config = Mock(SQLALCHEMY_DATABASE_URI='mock_db_uri')
        mock_engine = Mock()
        mock_create_engine.return_value = mock_engine
        mock_get_file_name_from_url.return_value = 'mock_file_name.csv'
        mock_get_list_of_urls_in_business_section.return_value = ['mock_url1.csv', 'mock_url2.zip']
        mock_download_file.return_value = 'mock_url2.zip'
        mock_unzip_file.return_value = ['mock_file1.csv', 'mock_file2.csv']
        main.main(mock_config)
        mock_logger_info.assert_called_with('was able to read csv file mock_file_name.csv')
        mock_create_engine.assert_called_with('mock_db_uri')
        mock_get_list_of_urls_in_business_section.assert_called_with(
            'https://www.stats.govt.nz/large-datasets/csv-files-for-download')
        mock_get_file_name_from_url.assert_called_with('mock_url2.zip')
        mock_create_table.assert_called_with(mock_read_csv.return_value, 'mock_file_name.csv', mock_engine)


if __name__ == '__main__':
    unittest.main()
