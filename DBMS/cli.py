import requests
import click
import json
import os
import jwt

BASE_URL = "http://127.0.0.1:5000"
CONFIG_FILE = "config.json"
SECRET_KEY = 'AbhiSoochonGa'

def get_current_db():
    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, 'r') as f:
            config = json.load(f)
            current_db = config.get('current_db')
            print(f"Current database: {current_db}")
            return current_db
    return None

def set_current_db(db_name):
    config = get_config()
    config['current_db'] = db_name
    with open(CONFIG_FILE, 'w') as f:
        json.dump(config, f)

def get_config():
    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, 'r') as f:
            return json.load(f)
    return {}

def save_config(config):
    with open(CONFIG_FILE, 'w') as f:
        json.dump(config, f)

def get_auth_token():
    config = get_config()
    return config.get('token')

@click.group()
def cli():
    pass

@click.command()
@click.argument('username')
@click.argument('password')
def login(username, password):
    """
    Login to the system.
    """
    response = requests.post(f'{BASE_URL}/login', json={'username': username, 'password': password})
    if response.status_code == 200:
        token = response.json().get('token')
        config = get_config()
        config['token'] = token
        save_config(config)
        click.echo("Login successful.")
    else:
        click.echo(f"Error: {response.json()['error']}")

@click.command()
@click.argument('username')
@click.argument('password')
def register(username, password):
    """
    Register a new user.
    """
    response = requests.post(f'{BASE_URL}/register', json={'username': username, 'password': password})
    if response.status_code == 201:
        click.echo("Registration successful.")
    else:
        click.echo(f"Error: {response.json()['error']}")

@click.command()
@click.argument('db_name')
def select_db(db_name):
    """
    Select a database. If the database doesn't exist, it will be created.
    """
    token = get_auth_token()
    headers = {'Authorization': f'Bearer {token}'}
    response = requests.post(f'{BASE_URL}/select_database', json={'db_name': db_name}, headers=headers)
    
    if response.status_code == 201 or response.status_code == 200:
        set_current_db(db_name)
        click.echo(f"Success: {response.json()['message']}")
    else:
        click.echo(f"Error: {response.json()['error']}")

@click.command()
@click.argument('table_name')
@click.argument('columns')   
@click.argument('datatypes')
@click.option('--constraints', default='', help='Constraints for the columns')
def create_table(table_name, columns, datatypes, constraints):
    """
    Create a new table in the selected database.
    """
    current_db = get_current_db()
    if current_db is None:
        click.echo("No database selected. Use the select_db command first.")
        return

    token = get_auth_token()
    headers = {'Authorization': f'Bearer {token}'}

    columns = columns.split(',')
    datatypes = datatypes.split(',')
    constraint_dict = {}
    
    if constraints:
        major_split = constraints.split(',')
        for item in major_split:
            key, value = item.split('=')
            constraint_dict[key] = value.split('|') if '|' in value else [value]
            
    response = requests.post(f'{BASE_URL}/create_table', json={
        'db_name': current_db,
        'table_name': table_name,
        'columns': columns,
        'datatypes': datatypes,
        'constraints': constraint_dict
    }, headers=headers)

    if response.status_code == 201 or response.status_code == 200:
        click.echo(f"Success: {response.json()['message']}")
    else:
        click.echo(f"Error: {response.json()['error']}")

@click.command()
@click.argument('table_name')
@click.argument('content')
def insert_record(table_name, content):
    """
    Insert a record into a table in the selected database.
    """
    current_db = get_current_db()
    if current_db is None:
        click.echo("No database selected. Use the select_db command first.")
        return

    token = get_auth_token()
    headers = {'Authorization': f'Bearer {token}'}

    content = content.split(',')
    response = requests.post(f'{BASE_URL}/insert_record', json={
        'db_name': current_db,
        'table_name': table_name,
        'content': content
    }, headers=headers)

    if response.status_code == 201 or response.status_code == 200:
        click.echo(f"Success: {response.json()['message']}")
    else:
        click.echo(f"Error: {response.json()['error']}")

@click.command()
@click.argument("table_name")
def select(table_name):
    """
    Select records from a table in the selected database.
    """
    current_db = get_current_db()
    if current_db is None:
        click.echo("No database selected. Use the select_db command first.")
        return

    token = get_auth_token()
    headers = {'Authorization': f'Bearer {token}'}

    response = requests.post(f'{BASE_URL}/select', json={
        'db_name': current_db,
        'table_name': table_name
    }, headers=headers)
    
    if response.status_code == 200 or response.status_code == 201:
        click.echo(f"Records: {response.json()['records']}")
    else:
        click.echo(f"Error: {response.json()['error']}")

@click.command()
@click.argument('table_name')
@click.argument('primary_key')
@click.argument('new_record')
def update_record(table_name, primary_key, new_record):
    """
    Update a record in a table in the selected database.
    """
    current_db = get_current_db()
    if current_db is None:
        click.echo("No database selected. Use the select_db command first.")
        return

    token = get_auth_token()
    headers = {'Authorization': f'Bearer {token}'}

    new_record = new_record.split(',')
    response = requests.put(f'{BASE_URL}/update_record', json={
        'db_name': current_db,
        'table_name': table_name,
        'primary_key': primary_key,
        'new_record': new_record
    }, headers=headers)

    if response.status_code == 200:
        click.echo(f"Success: {response.json()['message']}")
    else:
        click.echo(f"Error: {response.json()['error']}")

@click.command()
@click.argument('table_name')
@click.argument('primary_key')
def delete_record(table_name, primary_key):
    """
    Delete a record from a table in the selected database.
    """
    current_db = get_current_db() 
    if current_db is None:
        click.echo("No database selected. Use the select_db command first.")
        return

    token = get_auth_token()
    headers = {'Authorization': f'Bearer {token}'}

    response = requests.delete(f'{BASE_URL}/delete', json={
        'db_name': current_db,
        'table_name': table_name,
        'primary_key': primary_key
    }, headers=headers)
    
    if response.status_code == 200:
        click.echo(f"Success: {response.json()['message']}")
    else:
        click.echo(f"Error: {response.json()['error']}")

@click.command()
@click.argument('table_name')
def drop_table(table_name):
    """
    Delete a table from the selected database.
    """ 
    current_db = get_current_db()
    if current_db is None:
        click.echo("No database selected. Use the select_db command first.")
        return

    token = get_auth_token()
    headers = {'Authorization': f'Bearer {token}'}

    response = requests.delete(f'{BASE_URL}/drop_table', json={
        'db_name': current_db,
        'table_name': table_name
    }, headers=headers)
    
    if response.status_code == 200:
        click.echo(f"Success: {response.json()['message']}")
    else:
        click.echo(f"Error: {response.json()['error']}")        

# Add commands to the cli group
cli.add_command(login)
cli.add_command(register)
cli.add_command(select_db)
cli.add_command(create_table)
cli.add_command(insert_record)
cli.add_command(select)
cli.add_command(update_record)
cli.add_command(delete_record)
cli.add_command(drop_table)

if __name__ == '__main__':
    cli()
