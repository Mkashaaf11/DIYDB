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
    response  = requests.post(f'{BASE_URL}/create_table',json={'table_name': table_name})
    click.echo(response.json())
    
    

@click.command()
@click.argument('table_name') 
@click.argument('datatypes')
@click.argument('columns')   
@click.option('--constraints', default='', help= 'constraints for the columns')
def define_columns(table_name,columns,datatypes,constraint_arg):
    """
    Setting column names, their datatypes and constraint on the columns if any
    """
    columns = columns.split(',')
    datatypes = datatypes.split(',')
    constraints = {}
    if constraint_arg:
        majorsplit = constraints.split(',')
        for item in majorsplit:
            key,value = item.split('=')
            constraints[key] = value.split('|') if '|' in value else [value]
            
    
    response =  requests.post(f'{BASE_URL}/define_columns', json={'table_name':table_name,'datatypes':datatypes, 'columns':columns, 'constraints' : constraints })
    click.echo(response.json())
    

@click.command()
@click.argument('table_name') 
@click.argument('content') 
def insert_record(table_name,content):
    """
    insert the record in the table
    
    """
    response=requests.post(f'{BASE_URL}/insert_record',json = {'table_name':table_name,'content':content})  
    click.echo(response.json())
    

@click.command()
@click.argument("table_name")
def select(table_name):
    """
    return all records of the specified table
    
    """
    response = requests.post(f'{BASE_URL}/select',json={'table_name':table_name})
    click.echo(response.json())
    
@click.command()
@click.argument('table_name')
@click.argument('primary_key')
@click.argument('new_record')
def update_record(table_name,primary_key,new_record):
    """
    Update a record in the table.
    """
    response = requests.put(f'{BASE_URL}/update_record',json = {'table_name':table_name, 'primary_key':primary_key, 'new_record':new_record})  
    click.echo(response.json())      
    
@click.command()
@click.argument('table_name')
@click.argument('primary_key')
def delete_record(table_name,primary_key):
    """
    deleting the record
    
    """ 
    response = requests.delete(f'{BASE_URL}/delete',json = {'table_name':table_name, 'primary_key':primary_key})  
    click.echo(response.json())        

cli.add_command(create_table)
cli.add_command(define_columns)
cli.add_command(insert_record)
cli.add_command(select)
cli.add_command(update_record)
cli.add_command(delete_record)

if __name__ == '__main__':
    cli()    