import sys
import os
import pytest
import requests_mock

# Add the parent directory to the sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from click.testing import CliRunner
from cli import cli, set_database

BASE_URL = "http://127.0.0.1:5000"

@pytest.fixture
def runner():
    return CliRunner()

@pytest.fixture
def requests_mock_fixture():
    with requests_mock.Mocker() as m:
        yield m

def test_select_db(runner, requests_mock_fixture):
    db_name = 'test_db'
    requests_mock_fixture.post(f'{BASE_URL}/select_db', json={'message': f'Database {db_name} selected'}, status_code=200)
    result = runner.invoke(cli, ['select-db', db_name])
    print(result.output)
   
    assert result.exit_code == 0
    assert f"Database selected: {db_name}" in result.output

def test_create_table(runner, requests_mock_fixture):
    db_name = 'test_db'
    set_database(db_name)
    table_name = 'test_table'
    columns = 'id,name'
    datatypes = 'int,str'
    constraints = 'id=UNIQUE|NOT NULL'
    requests_mock_fixture.post(f'{BASE_URL}/create_table', json={'message': 'Table created successfully'}, status_code=201)
    result = runner.invoke(cli, ['create-table', table_name, columns, datatypes])
    print(result.output)
    
    assert result.exit_code == 0
    assert "Success: Table created successfully" in result.output

def test_insert_record(runner, requests_mock_fixture):
    db_name = 'test_db'
    set_database(db_name)
    table_name = 'test_table'
    content = '1,John Doe'
    requests_mock_fixture.post(f'{BASE_URL}/insert_record', json={'message': 'Record inserted successfully'}, status_code=201)
    result = runner.invoke(cli, ['insert-record', table_name, content])
    print(result.output)
    assert result.exit_code == 0
    assert "Success: Record inserted successfully" in result.output

def test_update_record(runner, requests_mock_fixture):
    db_name = 'test_db'
    set_database(db_name)
    table_name = 'test_table'
    primary_key = '1'
    new_record = '1,Jane Doe'
    requests_mock_fixture.put(f'{BASE_URL}/update_record', json={'message': 'Record updated successfully'}, status_code=200)
    result = runner.invoke(cli, ['update-record', table_name, primary_key, new_record])
    print(result.output)
   
    assert result.exit_code == 0
    assert "Success: Record updated successfully" in result.output

def test_delete_record(runner, requests_mock_fixture):
    db_name = 'test_db'
    set_database(db_name)
    table_name = 'test_table'
    primary_key = '1'
    requests_mock_fixture.delete(f'{BASE_URL}/delete', json={'message': 'Record deleted successfully'}, status_code=200)
    result = runner.invoke(cli, ['delete-record', table_name, primary_key])
    print(result.output)
    
    assert result.exit_code == 0
    assert "Success: Record deleted successfully" in result.output

def test_drop_table(runner, requests_mock_fixture):
    db_name = 'test_db'
    set_database(db_name)
    table_name = 'test_table'
    requests_mock_fixture.delete(f'{BASE_URL}/drop_table', json={'message': 'Table dropped successfully'}, status_code=200)
    result = runner.invoke(cli, ['drop-table', table_name])
    print(result.output)
    
    assert result.exit_code == 0
    assert "Success: Table dropped successfully" in result.output
