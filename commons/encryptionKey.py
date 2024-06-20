import os
from cryptography.fernet import Fernet


class encryptionKey:

    def create_key():
        encryption_key = f"{os.environ['PROJECT_HOME']}/output/encryption.key"
        if not os.path.exists(encryption_key):
            with open(encryption_key, 'wb') as fkey:
                fkey.write(Fernet.generate_key())

    def get_key():
        encryption_key = f"{os.environ['PROJECT_HOME']}/output/encryption.key"
        if os.path.exists(encryption_key):
            with open(encryption_key, 'rb') as fkey:
                fernet_key_file = fkey.read()
        else:
            encryptionKey.create_key()
            with open(encryption_key, 'rb') as fkey:
                fernet_key_file = fkey.read()

        fernetKey = Fernet(fernet_key_file)
        return fernetKey