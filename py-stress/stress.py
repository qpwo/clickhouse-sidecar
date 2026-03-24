import subprocess
import sys
import os
import clickhouse_sidecar

def main():
    print("Starting 20 concurrent Python workers. Some will crash intentionally.")
    procs = []
    worker_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "worker.py")
    for i in range(20):
        p = subprocess.Popen([sys.executable, worker_path, str(i)])
        procs.append(p)

    for p in procs:
        p.wait()

    print("All Python workers finished (or crashed). Daemon should drain and exit naturally in ~3 seconds.")

if __name__ == "__main__":
    main()
