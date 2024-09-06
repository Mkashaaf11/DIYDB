# DBMS/app.py

from flask import Flask, request, jsonify
from models.database import Database
from functools import wraps
from auth import Auth


app = Flask(__name__)
databases = {}
auth = Auth()

@app.route('/')
def home():
    """
    Home route for the Flask application.
    Returns a simple welcome message.
    """
    return 'Home Page'

def extract_token(headers):
    token = headers.get('x-access-token', '')
    if token.startswith('Bearer '):
        return token[len('Bearer '):]
    return token

def token_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        auth_header = request.headers.get('x-access-token')
        print(auth_header)
        if not auth_header:
            return jsonify({"error": "Token is missing. Login or register first"}), 401
        
        try:
            token = extract_token(request.headers)
            username = auth.verify_token(token)
            if "Invalid" in username or "expired" in username:
                return jsonify({"error": username}), 401
        except IndexError:
            return jsonify({"error": "Token format is incorrect. Ensure 'Bearer <token>' format."}), 401
        
        return f(*args, **kwargs)
    return decorated


@app.route('/register', methods=['POST'])
def register():
    data = request.json
    username = data.get('username')
    password = data.get('password')
    if not username or not password:
        return jsonify({"error": "Username and password are required"}), 400
    message = auth.register_user(username, password)
    if "successfully" in message:
        return jsonify({"message": message}), 201
    return jsonify({"error": message}), 400

@app.route('/login', methods=['POST'])
def login():
    data = request.json
    username = data.get('username')
    password = data.get('password')
    if not username or not password:
        return jsonify({"error": "Username and password are required"}), 400
    token = auth.authenticate_user(username, password)
    if "Invalid" in token:
        return jsonify({"error": token}), 401
    return jsonify({"token": token}), 200

@app.route('/select_database',methods = ['POST'])
@token_required
def select_database():
    data =  request.json
    db_name = data.get('db_name')
    token = extract_token(request.headers)
    username = auth.verify_token(token)
    print("app.py:",username)
    
    if not db_name:
        return jsonify({'error': "Database name is required"}),400
    
    if db_name in databases and databases[db_name].owner != username:
        return jsonify({'error': "Access denied"}), 403
    
    if db_name not in databases.keys():
        databases[db_name] = Database(db_name,username)
        #return jsonify({'message': f'Database {db_name} is created'}), 201
    
    return jsonify({"message": f"Databse {db_name} selected"}), 200
        


@app.route('/create_table', methods=['POST'])
@token_required
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
    
    print(f"Received db_name: {db_name}") 
    if not db_name or db_name not in databases.keys():
        return jsonify({"error":"Invalid database name"}), 400
    if not table_name or not columns or not datatypes:
        return jsonify({'error': 'Table name, columns , datatypes are required'}), 400
    
    db = databases[db_name]
    return jsonify({'message': db.create_table(table_name,columns,datatypes,constraints)})


@app.route('/insert_record', methods=['POST'])
@token_required
def insert_record():
    """
    Route to insert a record into a table.
    Expects JSON data with 'table_name' and 'content'.
    """
    data = request.json
    db_name = data.get('db_name')
    table_name = data.get('table_name')
    content = data.get('content')
    if not db_name or db_name not in databases.keys():
        return jsonify({"error":"Invalid database name"}), 400
    if not table_name or not content:
        return jsonify({'error': 'Table name and content are required'}), 400
    db = databases[db_name]
    result = db.insert(table_name, content)
    if result['success']:
        return jsonify({'message': result['message']}), 200
    else:
        return jsonify({'error': result['message']}), 400

@app.route('/select', methods=['POST'])
@token_required
def select():
    """
    Route to select records from a table.
    Expects JSON data with 'table_name'.
    """
    data = request.json
    db_name = data.get('db_name')
    table_name = data.get('table_name')
    if not db_name or db_name not in databases.keys():
        return jsonify({"error":"Invalid database name"}), 400
    
    if not table_name:
        return jsonify({'error': 'Table name is required'}), 400
    db = databases[db_name]
    return jsonify({'records': db.select_table(table_name)})

@app.route('/update_record', methods=['PUT'])
@token_required
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
    if not db_name or db_name not in databases.keys():
        return jsonify({"error":"Invalid database name"}), 400
    if not table_name or not primary_key or not new_record:
        return jsonify({'error': 'Table name, primary key, and new record are required'}), 400
    db = databases[db_name]
    return jsonify({'message': db.update(table_name, primary_key, new_record)})

@app.route('/delete', methods=['DELETE'])
@token_required
def delete():
    """
    Route to delete a record from a table.
    Expects JSON data with 'table_name' and 'primary_key'.
    """
    data = request.json
    db_name = data.get('db_name')
    table_name = data.get('table_name')
    primary_key = data.get('primary_key')
    if not db_name or db_name not in databases.keys():
        return jsonify({"error":"Invalid database name"}), 400
    if not table_name or not primary_key:
        return jsonify({'error': 'Table name and primary key are required'}), 400
    db = databases[db_name]
    return jsonify({'message': db.delete(table_name, primary_key)})

@app.route('/drop_table', methods=['POST'])
@token_required
def drop():
    """
    Route to delete a record from a table.
    Expects JSON data with 'table_name' and 'primary_key'.
    """
    data = request.json
    db_name = data.get('db_name')
    table_name = data.get('table_name')
    if not db_name or db_name not in databases.keys():
        return jsonify({"error":"Invalid database name"}), 400
    if not table_name:
        return jsonify({'error': 'Table name is required'}), 400
    db = databases[db_name]
    return jsonify({'message': db.drop_table(table_name)})

if __name__ == '__main__':
    app.run(debug=True)
