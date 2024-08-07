# auth.py
import os
import bcrypt
import json
import jwt
from datetime import datetime, timedelta

USER_FILE_PATH = 'user_credentials.json'
SECRET_KEY = 'AbhiSoochonGa'

class Auth:
    def __init__(self):
        self.users = self.load_users()

    def load_users(self):
        if not os.path.exists(USER_FILE_PATH):
            return []
        with open(USER_FILE_PATH, 'r') as file:
            return json.load(file).get('users', [])

    def save_users(self):
        with open(USER_FILE_PATH, 'w') as file:
            json.dump({'users': self.users}, file, indent=4)

    def hash_password(self, password: str) -> str:
        salt = bcrypt.gensalt()
        hashed_password = bcrypt.hashpw(password.encode('utf-8'), salt)
        return hashed_password.decode('utf-8')

    def convert_password(self, password: str, hashed_password: str) -> bool:
        return bcrypt.checkpw(password.encode('utf-8'), hashed_password.encode('utf-8'))

    def register_user(self, username: str, password: str):
        if any(user['username'] == username for user in self.users):
            return f'{username} already exists'
        hash_password = self.hash_password(password)
        self.users.append({'username': username, 'password': hash_password})
        self.save_users()
        return 'User created successfully'

    def authenticate_user(self, username: str, password: str):
        for user in self.users:
            if user['username'] == username:
                if self.convert_password(password, user['password']):
                    token = jwt.encode({
                        "username": username,
                        "exp": datetime.utcnow() + timedelta(hours=1)
                    }, SECRET_KEY, algorithm="HS256")
                    return token
        return 'Invalid credentials'

    def verify_token(self, token):
        try:
            payload = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
            return payload['username']
        except jwt.ExpiredSignatureError:
            return "Token expired"
        except jwt.InvalidTokenError:
            return "Invalid token"
