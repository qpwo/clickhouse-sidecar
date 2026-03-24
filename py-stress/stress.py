"""Rigorous stress test spawning 20 workers, some of which will intentionally crash."""
import subprocess
import sys
import clickhouse_sidecar

def main():
    print("Starting 20 concurrent Python workers. Some will crash intentionally.")
    procs = []
    for i in range(20):
        p = subprocess.Popen([sys.executable, "py-stress/worker.py", str(i)])
        procs.append(p)

    for p in procs:
        p.wait()

    print("All Python workers finished (or crashed). Daemon should drain and exit naturally in ~3 seconds.")

if __name__ == "__main__":
    main()
