"""a oneline docstring"""
import subprocess, sys

def main():
    procs = []
    print("Spawning workers...")
    for i in range(5):
        p = subprocess.Popen([sys.executable, "tests/worker.py", str(i)])
        procs.append(p)
    for p in procs:
        p.wait()
    print("All workers finished. Daemon should shut down shortly after.")

if __name__ == "__main__":
    main()
