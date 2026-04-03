import subprocess
proc = subprocess.run(["/Users/ubuntu/.shared-clickhouse-bin/clickhouse", "server", "--help"], capture_output=True)
