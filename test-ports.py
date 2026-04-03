import os, subprocess, time, urllib.request, urllib.parse, socket
bin_path = os.path.expanduser("~/.shared-clickhouse-bin/clickhouse")
d = "test-ports-db"
os.makedirs(d, exist_ok=True)
os.makedirs(d+"/data", exist_ok=True)
os.makedirs(d+"/tmp", exist_ok=True)
os.makedirs(d+"/user_files", exist_ok=True)

conf = f"""<clickhouse>
    <logger><level>none</level><console>false</console></logger>
    <!-- no tcp_port, no http_port -->
    <http_port></http_port>
    <tcp_port></tcp_port>
    <listen_host>127.0.0.1</listen_host>
    <path>{d}/data/</path>
    <tmp_path>{d}/tmp/</tmp_path>
    <user_files_path>{d}/user_files/</user_files_path>
    <users_config>{d}/users.xml</users_config>
</clickhouse>"""
open(d+"/config.xml", "w").write(conf)
open(d+"/users.xml", "w").write("<clickhouse><profiles><default/></profiles><users><default><password></password><networks><ip>127.0.0.1</ip><ip>::1</ip></networks><profile>default</profile><quota>default</quota><access_management>1</access_management></default></users><quotas><default/></quotas></clickhouse>")

proc = subprocess.Popen([bin_path, "server", "--config-file", f"{d}/config.xml"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
time.sleep(2)
print("Running. PID:", proc.pid)
# Check what ports it is listening on
subprocess.run(["lsof", "-Pni", f"-p{proc.pid}"])
proc.kill()
