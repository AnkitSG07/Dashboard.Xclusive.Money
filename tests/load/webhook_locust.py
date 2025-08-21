from locust import HttpUser, task, between

class WebhookUser(HttpUser):
    """Simulate high-rate webhook POSTs."""
    wait_time = between(0.1, 0.5)

    @task
    def send_webhook(self):
        payload = {"event": "trade", "data": {"id": 1, "symbol": "XYZ"}}
        self.client.post("/api/webhook", json=payload)
