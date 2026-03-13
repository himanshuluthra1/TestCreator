"""
WSGI configuration for PythonAnywhere deployment.

Instructions:
1. After cloning your repo on PythonAnywhere, go to:
   Web tab → WSGI configuration file → click the link to edit it
2. Replace the entire contents of that file with this file's contents
3. Update YOUR_USERNAME below to your actual PythonAnywhere username
"""

import sys
import os

# Path to your project on PythonAnywhere
path = '/home/YOUR_USERNAME/TestCreator'
if path not in sys.path:
    sys.path.insert(0, path)

# Set a strong secret key for Flask sessions
os.environ.setdefault('SECRET_KEY', 'CHANGE_ME_TO_A_LONG_RANDOM_SECRET_KEY')

from app import app as application  # noqa: E402
