CREATE OR REPLACE FUNCTION decrpt_data(
    encrypted_data STRING,
    fernet_key STRING
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION='3.8'
PACKAGES=('cryptography')
HANDLER= 'decrypt_data'
AS
$$
#
from cryptography.fernet import Fernet
def decrypt_data(encrypted_data, fernet_key):
  # Convert the fernet_key string to bytes
  fernet_key_bytes = fernet_key.encode('utf-8')

  # Initialize Fernet with the provided key
  cipher_suite = Fernet(fernet_key_bytes)

  # Decrypt the encrypted_data
  decrypted_data = cipher_suite.decrypt(encrypted_data.encode('utf-8')).decode('utf-8')

  return decrypted_data

$$;

