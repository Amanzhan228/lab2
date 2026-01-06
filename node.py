import http.server
import socketserver
import json
import urllib.parse
import requests
import time
import argparse


class Node:
    def __init__(self, node_id: str, port: int, peers: list[str]):
        self.id = node_id
        self.port = port
        self.peers = peers                  # e.g. ["http://172.31.x.x:8001", ...]
        self.clock = 0
        self.store = {}                     # key → (value, timestamp)

        print(f"[{self.id}] Node started on port {self.port} | Peers: {self.peers}")

    def increment_clock(self) -> int:
        self.clock += 1
        return self.clock

    def update_clock(self, received_ts: int):
        old = self.clock
        self.clock = max(self.clock, received_ts) + 1
        print(f"[{self.id}] Clock updated: {old} → {self.clock} (received ts={received_ts})")

    def put(self, key: str, value: str):
        ts = self.increment_clock()
        self.store[key] = (value, ts)
        print(f"[{self.id}] LOCAL PUT {key} = {value} @ ts={ts} (clock={self.clock})")
        self.replicate_to_peers(key, value, ts)

    def replicate_to_peers(self, key: str, value: str, ts: int):
        payload = {
            "type": "replicate",
            "key": key,
            "value": value,
            "timestamp": ts,
            "sender_id": self.id
        }

        for peer_url in self.peers:
            # Scenario A: artificial 8-second delay only from A → C
            if self.id == "A" and ":8002" in peer_url:
                print(f"[{self.id}] Applying 8-second delay to {peer_url}")
                time.sleep(8)

            # Retry up to 3 times with exponential backoff
            for attempt in range(3):
                try:
                    requests.post(peer_url + "/replicate", json=payload, timeout=5)
                    print(f"[{self.id}] Successfully replicated {key} → {peer_url} @ ts={ts}")
                    break
                except Exception as e:
                    print(f"[{self.id}] Replication failed to {peer_url} (attempt {attempt + 1}/3): {e}")
                    if attempt < 2:
                        time.sleep(2 ** attempt)   # 1s, then 2s
                    else:
                        print(f"[{self.id}] Giving up on {peer_url}")

    def handle_replicate(self, data: dict):
        key = data["key"]
        value = data["value"]
        ts = data["timestamp"]
        sender = data["sender_id"]

        self.update_clock(ts)

        current_val, current_ts = self.store.get(key, (None, -1))

        # Last-Writer-Wins + tie-breaker by node ID (C > B > A)
        should_update = ts > current_ts or (ts == current_ts and sender > self.id)

        if should_update:
            self.store[key] = (value, ts)
            print(f"[{self.id}] ACCEPTED {key} = {value} @ ts={ts} from {sender} (LWW win)")
        else:
            print(f"[{self.id}] IGNORED {key} @ ts={ts} from {sender} (stale or lost tie)")

    def status(self) -> dict:
        return {
            "node_id": self.id,
            "clock": self.clock,
            "store": {k: {"value": v, "timestamp": ts} for k, (v, ts) in self.store.items()}
        }


# Global instance used by the HTTP handler
node = None


class Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        global node
        if self.path == "/status":
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps(node.status(), indent=2).encode())
        else:
            self.send_response(404)
            self.end_headers()

    def do_POST(self):
        global node
        content_length = int(self.headers.get("Content-Length", 0))
        body = json.loads(self.rfile.read(content_length))

        if self.path == "/put":
            key = body["key"]
            value = body["value"]
            node.put(key, value)
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"OK\n")

        elif self.path == "/replicate":
            node.handle_replicate(body)
            self.send_response(200)
            self.end_headers()

        else:
            self.send_response(404)
            self.end_headers()


def main():
    global node
    parser = argparse.ArgumentParser()
    parser.add_argument("--id", required=True, help="Node ID: A, B or C")
    parser.add_argument("--port", type=int, required=True, help="Port to listen on")
    parser.add_argument("--peers", required=True, help="Comma-separated peer URLs")
    args = parser.parse_args()

    node = Node(args.id, args.port, args.peers.split(","))

    print(f"[{node.id}] Server running on port {args.port}")
    with socketserver.TCPServer(("", args.port), Handler) as httpd:
        httpd.serve_forever()


if __name__ == "__main__":
    main()
