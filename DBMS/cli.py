#DBMS/cli.py

import requests
import click
import json
import os

BASE_URL = "http://127.0.0.1:5000"
CONFIG_FILE = "config.json"

def get_current_db():
    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, 'r') as f:
            config = json.load(f)
            current_db = config.get('current_db')
            print(f"Current database: {current_db}")
            return config.get('current_db')
    return None

def set_current_db(db_name):
    config = {'current_db': db_name}
    with open(CONFIG_FILE, 'w') as f:
        json.dump(config, f)

@click.group()
def cli():
    pass

@click.command()
@click.argument('db_name')
def select_db(db_name):
    """
    Select a database. If the database doesn't exist, it will be created.
    """
    response = requests.post(f'{BASE_URL}/select_database', json={'db_name': db_name})
    
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
    })

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

    content = content.split(',')
    response = requests.post(f'{BASE_URL}/insert_record', json={
        'db_name': current_db,
        'table_name': table_name,
        'content': content
    })

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

    response = requests.post(f'{BASE_URL}/select', json={
        'db_name': current_db,
        'table_name': table_name
    })
    
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

    new_record = new_record.split(',')
    response = requests.put(f'{BASE_URL}/update_record', json={
        'db_name': current_db,
        'table_name': table_name,
        'primary_key': primary_key,
        'new_record': new_record
    })

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

    response = requests.delete(f'{BASE_URL}/delete', json={
        'db_name': current_db,
        'table_name': table_name,
        'primary_key': primary_key
    })
    
    if response.status_code == 200:
        click.echo(f"Success: {response.json()['message']}")
    else:
        click.echo(f"Error: {response.json()['error']}")

@click.command()
@click.argument('table_name')
def drop_table(table_name):
    """
    Delete a record from a table in the selected database.
    """ 
    current_db = get_current_db()
    if current_db is None:
        click.echo("No database selected. Use the select_db command first.")
        return

    response = requests.delete(f'{BASE_URL}/drop_table', json={
        'db_name': current_db,
        'table_name': table_name
    })
    
    if response.status_code == 200:
        click.echo(f"Success: {response.json()['message']}")
    else:
        click.echo(f"Error: {response.json()['error']}")        

# Add commands to the cli group
cli.add_command(select_db)
cli.add_command(create_table)
cli.add_command(insert_record)
cli.add_command(select)
cli.add_command(update_record)
cli.add_command(delete_record)
cli.add_command(drop_table)

if __name__ == '__main__':
    cli()
