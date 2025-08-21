from locust import HttpUser, task, between

class CopyTradeUser(HttpUser):
    """Simulate rapid copy-trade requests."""
    wait_time = between(0.5, 1.0)

    @task
    def copy_trade(self):
        payload = {"source_user": "leader", "trade_id": 123, "quantity": 10}
        self.client.post("/api/copy-trade", json=payload)
