"""
Passenger WSGI entry point for Hostinger shared hosting.

Hostinger uses Phusion Passenger to serve Python/WSGI apps.
This file must be named passenger_wsgi.py and placed in the
domain/application root directory.
"""

import sys
import os

# Add the application directory to the Python path
INTERP = os.path.expanduser("~/virtualenv/testcreator/3.11/bin/python3")
if sys.executable != INTERP:
    os.execl(INTERP, INTERP, *sys.argv)

# Ensure the app directory is on the path
app_dir = os.path.dirname(os.path.abspath(__file__))
if app_dir not in sys.path:
    sys.path.insert(0, app_dir)

# Set a stable secret key for sessions (override via environment variable in production)
if not os.environ.get("SECRET_KEY"):
    os.environ["SECRET_KEY"] = "CHANGE_ME_TO_A_LONG_RANDOM_SECRET_KEY"

from app import app as application  # noqa: E402
