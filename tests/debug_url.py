import urllib.request
import clickhouse_sidecar
import re
import traceback

def main():
    client = clickhouse_sidecar.get_client("debug-db")
    with open("debug-db/config.xml") as f:
        config = f.read()
    port = re.search(r"<http_port>(\d+)</http_port>", config).group(1)

    url = f"http://127.0.0.1:{port}/?query=SELECT+count()+FROM+system.processes+WHERE+is_initial_query=1+AND+query+NOT+LIKE+'%system.processes%'"
    print("Testing URL:", url)
    try:
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req) as res:
            print("Response:", res.read().decode())
    except Exception as e:
        print("Error!")
        traceback.print_exc()

if __name__ == "__main__":
    main()
