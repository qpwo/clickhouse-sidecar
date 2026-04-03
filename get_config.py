import subprocess
proc = subprocess.run(["/Users/ubuntu/.shared-clickhouse-bin/clickhouse", "server", "--help"], capture_output=True)
proc2 = subprocess.run(["/Users/ubuntu/.shared-clickhouse-bin/clickhouse", "extract-from-config", "--config-file", "./tmp-ch/config.xml"], capture_output=True)
