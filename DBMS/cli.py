import requests
import click

BASE_URL = "http://127.0.0.1:5000"

@click.group()
def cli():
    pass

@click.command()
@click.argument('table_name')
def create_table(table_name):
    """
    Creating table by taking table name as argument 
    The argument is passed to the create table flask BASE_URL
    """
    response  = requests.post(f'{BASE_URL}/create_table', json={'table_name': table_name})
    click.echo(response.json())

@click.command()
@click.argument('table_name') 
@click.argument('columns')   
@click.argument('datatypes')
@click.option('--constraints', default='', help='constraints for the columns')
def define_columns(table_name, columns, datatypes, constraints):
    """
    Setting column names, their datatypes and constraint on the columns if any
    """
    columns = columns.split(',')
    datatypes = datatypes.split(',')
    constraint_dict = {}
    if constraints:
        major_split = constraints.split(',')
        for item in major_split:
            key, value = item.split('=')
            constraint_dict[key] = value.split('|') if '|' in value else [value]
    
    response = requests.post(f'{BASE_URL}/define_columns', json={'table_name': table_name, 'columns': columns, 'datatypes': datatypes, 'constraints': constraint_dict})
    click.echo(response.text)
    click.echo(response.json())

@click.command()
@click.argument('table_name') 
@click.argument('content') 
def insert_record(table_name, content):
    """
    Taking the table name as argument and inserting the record in the table
    """
    content = content.split(',')
    response = requests.post(f'{BASE_URL}/insert_record', json={'table_name': table_name, 'content': content}) 
    click.echo(response.text) 
    click.echo(response.json())

@click.command()
@click.argument("table_name")
def select(table_name):
    """
    Taking table name as argument and returning all records of that table
    """
    response = requests.post(f'{BASE_URL}/select', json={'table_name': table_name})
    click.echo(response.json())

@click.command()
@click.argument('table_name')
@click.argument('primary_key')
@click.argument('new_record')
def update_record(table_name, primary_key, new_record):
    """
    Taking table name, primary key and new record which is to be inserted as argument
    New record received as a list is inserted in the table
    """
    new_record = new_record.split(',')
    response = requests.put(f'{BASE_URL}/update_record', json={'table_name': table_name, 'primary_key': primary_key, 'new_record': new_record})  
    click.echo(response.json())      

@click.command()
@click.argument('table_name')
@click.argument('primary_key')
def delete_record(table_name, primary_key):
    """
    Deleting the record having primary key provided in argument in table provided as table name
    """ 
    response = requests.delete(f'{BASE_URL}/delete', json={'table_name': table_name, 'primary_key': primary_key})  
    click.echo(response.json())        

# Add commands to the cli group
cli.add_command(create_table)
cli.add_command(define_columns)
cli.add_command(insert_record)
cli.add_command(select)
cli.add_command(update_record)
cli.add_command(delete_record)

if __name__ == '__main__':
    cli()
