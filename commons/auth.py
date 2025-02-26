import traceback
import hashlib

class userAuth:
    @staticmethod
    def get_hashed_password(cursor, username):
        try:
            cursor.execute("SELECT password FROM users WHERE username = %s", (username,))
            result = cursor.fetchone()
            if result:
                return result[0].tobytes()
            return None
        except Exception as e:
            return traceback.format_exc()

    @staticmethod
    def verify_user(cursor, username, password, fernet_key):
        if password == fernet_key.decrypt(userAuth.get_hashed_password(cursor, username)).decode():
            return True
        else:
            return False