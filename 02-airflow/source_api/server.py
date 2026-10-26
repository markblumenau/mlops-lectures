import json
import os
import random
import time
from threading import Lock
from datetime import date, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

CITIES = ["Москва", "Санкт-Петербург", "Казань", "Новосибирск", "Екатеринбург"]
CHANNELS = ["web", "mobile", "partner"]
FLAKY_RATE = float(os.environ.get("FLAKY_RATE", "0"))
LATENCY_MS = int(os.environ.get("LATENCY_MS", "0"))
FAIL_FIRST = int(os.environ.get("FAIL_FIRST", "0"))
SOURCE_READY = os.environ.get("SOURCE_READY", "1") == "1"
ATTEMPTS: dict[str, int] = {}
ATTEMPTS_LOCK = Lock()


def make_orders(day: date) -> list[dict]:
    rnd = random.Random(day.toordinal())
    n = rnd.randint(40, 60) if day.weekday() >= 5 else rnd.randint(20, 40)
    orders = []
    for i in range(n):
        amount = round(rnd.uniform(300, 15000), 2)
        orders.append(
            {
                "order_id": f"{day:%Y%m%d}-{i:04d}",
                "created_at": f"{day}T{rnd.randint(0, 23):02d}:{rnd.randint(0, 59):02d}:00",
                "city": rnd.choice(CITIES),
                "channel": rnd.choice(CHANNELS),
                "items": rnd.randint(1, 5),
                "amount": amount,
            }
        )
    for broken in rnd.sample(orders, 2):
        broken["amount"] = None
    return orders


class Handler(BaseHTTPRequestHandler):
    def _send(self, code: int, payload: dict) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:  # noqa: N802
        url = urlparse(self.path)
        if url.path == "/health":
            self._send(200 if SOURCE_READY else 404, {"status": "ok" if SOURCE_READY else "not ready"})
            return
        if url.path != "/orders":
            self._send(404, {"error": "not found"})
            return

        if LATENCY_MS:
            time.sleep(LATENCY_MS / 1000)
        if FLAKY_RATE and random.random() < FLAKY_RATE:
            self._send(503, {"error": "service unavailable"})
            return

        raw = parse_qs(url.query).get("date", [""])[0]
        try:
            day = datetime.strptime(raw, "%Y-%m-%d").date()
        except ValueError:
            self._send(400, {"error": "date=YYYY-MM-DD is required"})
            return
        with ATTEMPTS_LOCK:
            ATTEMPTS[raw] = ATTEMPTS.get(raw, 0) + 1
            attempt = ATTEMPTS[raw]
        if attempt <= FAIL_FIRST:
            self._send(503, {"error": "planned temporary failure", "attempt": attempt})
            return
        if day > date.today():
            self._send(404, {"error": f"no data for {day} yet"})
            return
        self._send(200, {"date": str(day), "orders": make_orders(day)})

    def log_message(self, fmt: str, *args) -> None:
        print(f"[source-api] {fmt % args}", flush=True)


if __name__ == "__main__":
    ThreadingHTTPServer(("0.0.0.0", 8000), Handler).serve_forever()
