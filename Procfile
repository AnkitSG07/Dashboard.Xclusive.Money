web: gunicorn wsgi:application
scheduler: celery -A services.scheduler beat --loglevel=info
worker: celery -A task.celery worker --loglevel=info
order-consumer: python -m services.order_consumer
trade-copier: python -m services.trade_copier
master-trade-monitor: python -m services.master_trade_monitor
