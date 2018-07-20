"""
    Entry script to start Epiviz File Server
"""

from server import app

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, workers=1)
    