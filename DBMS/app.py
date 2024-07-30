# DBMS/app.py

from flask import Flask, request, jsonify
from models.database import Database

app = Flask(__name__)
databases = {}

@app.route('/')
def home():
    """
    Home route for the Flask application.
    Returns a simple welcome message.
    """
    return 'Home Page'

@app.route('/select_database',methods = ['POST'])
def select_database():
    data =  request.json
    db_name = data.get('db_name')
    
    if not db_name:
        return jsonify({'error': "Database name is required"}),400
    if db_name not in databases.keys:
        databases[db_name] = Database(db_name)
        return jsonify({'message': f'Database {db_name} is created'}), 201
    
    return jsonify({"message": f"Databse {db_name} selected"}), 200
        


@app.route('/create_table', methods=['POST'])
def create_table():
    """
    Route to create a new table.
    Expects JSON data with 'table_name'.
    """
    data = request.json
    db_name = data.get('db_name')
    table_name = data.get('table_name')
    columns = data.get('columns')
    datatypes = data.get('datatypes')
    constraints = data.get('constraints', {})
    if not db_name or db_name not in databases.keys:
        return jsonify({"error":"Invalid database name"}), 400
    if not table_name or not columns or not datatypes:
        return jsonify({'error': 'Table name, columns , datatypes are required'}), 400
    
    db = databases[db_name]
    return jsonify({'message': db.create_table(table_name,columns,datatypes,constraints)})


@app.route('/insert_record', methods=['POST'])
def insert_record():
    """
    Route to insert a record into a table.
    Expects JSON data with 'table_name' and 'content'.
    """
    data = request.json
    db_name = data.get('db_name')
    table_name = data.get('table_name')
    content = data.get('content')
    if not db_name or db_name not in databases.keys:
        return jsonify({"error":"Invalid database name"}), 400
    if not table_name or not content:
        return jsonify({'error': 'Table name and content are required'}), 400
    db = databases[db_name]
    return jsonify({'message': db.insert(table_name, content)})

@app.route('/select', methods=['POST'])
def select():
    """
    Route to select records from a table.
    Expects JSON data with 'table_name'.
    """
    data = request.json
    db_name = data.get('db_name')
    table_name = data.get('table_name')
    if not db_name or db_name not in databases.keys:
        return jsonify({"error":"Invalid database name"}), 400
    
    if not table_name:
        return jsonify({'error': 'Table name is required'}), 400
    db = databases[db_name]
    return jsonify({'message': db.select_table(table_name)})

@app.route('/update_record', methods=['PUT'])
def update_record():
    """
    Route to update a record in a table.
    Expects JSON data with 'table_name', 'primary_key', and 'new_record'.
    """
    data = request.json
    db_name = data.get('db_name')
    table_name = data.get('table_name')
    primary_key = data.get('primary_key')
    new_record = data.get('new_record')
    if not db_name or db_name not in databases.keys:
        return jsonify({"error":"Invalid database name"}), 400
    if not table_name or not primary_key or not new_record:
        return jsonify({'error': 'Table name, primary key, and new record are required'}), 400
    db = databases[db_name]
    return jsonify({'message': db.update(table_name, primary_key, new_record)})

@app.route('/delete', methods=['DELETE'])
def delete():
    """
    Route to delete a record from a table.
    Expects JSON data with 'table_name' and 'primary_key'.
    """
    data = request.json
    db_name = data.get('db_name')
    table_name = data.get('table_name')
    primary_key = data.get('primary_key')
    if not db_name or db_name not in databases.keys:
        return jsonify({"error":"Invalid database name"}), 400
    if not table_name or not primary_key:
        return jsonify({'error': 'Table name and primary key are required'}), 400
    db = databases[db_name]
    return jsonify({'message': db.delete(table_name, primary_key)})

@app.route('/drop_table', methods=['POST'])
def drop():
    """
    Route to delete a record from a table.
    Expects JSON data with 'table_name' and 'primary_key'.
    """
    data = request.json
    db_name = data.get('db_name')
    table_name = data.get('table_name')
    if not db_name or db_name not in databases.keys:
        return jsonify({"error":"Invalid database name"}), 400
    if not table_name:
        return jsonify({'error': 'Table name is required'}), 400
    db = databases[db_name]
    return jsonify({'message': db.drop_table(table_name)})

if __name__ == '__main__':
    app.run(debug=True)
