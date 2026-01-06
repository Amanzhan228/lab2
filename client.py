import requests
import argparse
import json
import sys


def main():
    parser = argparse.ArgumentParser(description="Simple client for the replicated key-value store")
    parser.add_argument("--node", required=True, help="Base URL of the node, e.g. http://172.31.19.162:8000")
    parser.add_argument("command", choices=["put", "get", "status"], help="Operation to perform")
    parser.add_argument("key", nargs="?", help="Key for put/get commands")
    parser.add_argument("value", nargs="?", help="Value for put command")

    args = parser.parse_args()

    base_url = args.node.rstrip("/")

    try:
        if args.command == "put":
            if not args.key or args.value is None:
                print("Error: 'put' command requires both key and value")
                sys.exit(1)

            response = requests.post(
                f"{base_url}/put",
                json={"key": args.key, "value": args.value},
                timeout=10
            )
            if response.status_code == 200:
                print(f"PUT {args.key} = {args.value} sent successfully")
            else:
                print(f"Error: Server returned {response.status_code}")

        elif args.command == "get":
            if not args.key:
                print("Error: 'get' command requires a key")
                sys.exit(1)

            response = requests.get(f"{base_url}/status", timeout=10)
            if response.status_code != 200:
                print(f"Error: Could not reach node (status {response.status_code})")
                return

            data = response.json()
            store = data.get("store", {})

            entry = store.get(args.key)
            if entry:
                print(f"{args.key} = {entry['value']}  (timestamp: {entry['timestamp']})")
            else:
                print(f"{args.key}: Not found")

        elif args.command == "status":
            response = requests.get(f"{base_url}/status", timeout=10)
            if response.status_code != 200:
                print(f"Error: Could not reach node (status {response.status_code})")
                return

            data = response.json()
            print(json.dumps(data, indent=2))

    except requests.exceptions.Timeout:
        print("Error: Request timed out – node may be unreachable")
    except requests.exceptions.ConnectionError:
        print("Error: Could not connect to the node – check URL and network")
    except requests.exceptions.RequestException as e:
        print(f"Error: Request failed: {e}")
    except json.JSONDecodeError:
        print("Error: Invalid JSON received from server")


if __name__ == "__main__":
    main()
