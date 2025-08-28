web: gunicorn wsgi:application
scheduler: celery -A services.scheduler beat --loglevel=info
worker: celery -A task.celery worker --loglevel=info
