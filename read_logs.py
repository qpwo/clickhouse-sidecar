import os
for db in ['stress-py-db', 'stress-node-db']:
    log_path = os.path.join(db, 'daemon.log')
    print(f"\n--- {log_path} ---")
    if os.path.exists(log_path):
        with open(log_path, 'r') as f:
            print(f.read())
    else:
        print("Not found.")
