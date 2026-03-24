import gc
import weakref
import clickhouse_sidecar

def main():
    def get_ref():
        client = clickhouse_sidecar.get_client("debug-db")
        return weakref.ref(client), weakref.ref(client._lease_sock)

    c_ref, s_ref = get_ref()
    gc.collect()
    print("Client collected?", c_ref() is None)
    print("Socket collected?", s_ref() is None)

if __name__ == "__main__":
    main()
