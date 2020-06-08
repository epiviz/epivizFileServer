============
Deployment
============


Using In built Sanic server (for development)
==============================================

Sanic provides a default asynchronous web server to run the API. As a working example, checkout the Roadmap project
from the Usecases section.

.. code-block:: python

    app = setup_app(mMgr)
    app.run("0.0.0.0", port=8000)


Deploy using gunicorn + supervisor (for production)
===================================================

Setup virtualenv and API
------------------------

This process assumes the root API directory is `/var/www/epiviz-api`

Setup virtualenv either through pip or conda

.. code-block:: console

    cd /var/www/epiviz-api
    virtualenv env
    source env/bin/activate
    pip install epivizfileserver

A generic version of the API script would look something like this 
(add this to `/var/www/epiviz-api/epiviz.py`)

.. code-block:: python

    from epivizfileserver import setup_app, create_fileHandler, MeasurementManager
    from epivizfileserver.trackhub import TrackHub

    # create measurements to load multiple trackhubs or configuration files
    mMgr = MeasurementManager()

    # create file handler, enables parallel processing of multiple requests
    mHandler = create_fileHandler()

    # add genome. - for supported genomes 
    # check https://obj.umiacs.umd.edu/genomes/index.html
    genome = mMgr.add_genome("mm10")
    genome = mMgr.add_genome("hg19")

    # load measurements/files through config or TrackHub

    # setup the app from the measurements manager 
    # and run the app
    app = setup_app(mMgr)

    # only if this file is run directly! 
    if __name__ == "__main__":
        app.run(host="127.0.0.1", port=8000)


Install dependencies 
--------------------

1. Supervisor (system wide) - http://supervisord.org/
2. Gunicorn (to the virtual environment) - https://gunicorn.org/

.. code-block:: console

    # if using ubuntu
    sudo apt install supervisor

    # activate virtualenv that runs the API
    source /var/www/epiviz-api/env/bin/activate
    pip install gunicorn


Configure supervisor
--------------------

Add this configuration to `/etc/supervisor/conf.d/epiviz.conf`

This snippet also assumes epiviz-api repo is in `/var/www/epiviz-api`

.. code-block:: console

    [program:gunicorn]
    directory=/var/www/epiviz-api
    environment=PYTHONPATH=/var/www/epiviz-api/bin/python
    command=/var/www/epiviz-api/env/bin/gunicorn epiviz:app --log-level debug --bind 0.0.0.0:8000 --worker-class sanic.worker.GunicornWorker
    autostart=true
    autorestart=true
    stderr_logfile=/var/log/gunicorn/gunicorn.err.log
    stdout_logfile=/var/log/gunicorn/gunicorn.out.log

Enable Supervisor configuration

.. code-block:: console

    sudo supervisorctl reread
    sudo supervisorctl update

    service supervisor restart 

.. note::

    check status of supervisor to make sure there are no errors


Add Proxypass to nginx/Apache
-----------------------------

(the port number here should match the binding port from supervisor configuration 


for Apache 

.. code-block:: console

    sudo a2enmod proxy
    sudo a2enmod proxy-http

    # add this to the apache site config
    ProxyPreserveHost On
    <Location "/api">
        ProxyPass "http://127.0.0.1:8000/"
        ProxyPassReverse "http://127.0.0.1:8000/"
    </Location>

for nginx


.. code-block:: console

    # add this to nginx site config 

    upstream epiviz_api_server {
        server 127.0.0.1:8000 fail_timeout=0;
    }

    location /api/ {
        proxy_pass http://epiviz_api_server/;
        proxy_set_header Host $host;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_redirect off;
    }