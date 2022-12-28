import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
import os.path

my_path = os.path.abspath(os.path.dirname(__file__))
path = os.path.join(my_path, '../firebase-secrets.json')

# Use a service account
cred = credentials.Certificate(path)
firebase_admin.initialize_app(cred)

firebaseDB = firestore.client()