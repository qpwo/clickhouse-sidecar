import subprocess, os
conf = """<clickhouse>
<logger><level>trace</level><console>true</console></logger>
<listen_host>127.0.0.1</listen_host>
<tcp_port></tcp_port>
<http_port></http_port>
<mysql_port></mysql_port>
<postgresql_port></postgresql_port>
<interserver_http_port></interserver_http_port>
<tcp_port_secure></tcp_port_secure>
<http_port_secure></http_port_secure>
<grpc_port></grpc_port>
<prometheus_port></prometheus_port>
</clickhouse>"""
users = "<clickhouse><profiles><default/></profiles><users><default><password></password><networks><ip>127.0.0.1</ip><ip>::1</ip></networks><profile>default</profile><quota>default</quota><access_management>1</access_management></default></users><quotas><default/></quotas></clickhouse>"
os.makedirs("./tmp-ch4/data", exist_ok=True)
os.makedirs("./tmp-ch4/tmp", exist_ok=True)
os.makedirs("./tmp-ch4/user_files", exist_ok=True)
open("./tmp-ch4/config.xml", "w").write(conf)
open("./tmp-ch4/users.xml", "w").write(users)

proc = subprocess.Popen(["/Users/ubuntu/.shared-clickhouse-bin/clickhouse", "server", "--config-file", "./tmp-ch4/config.xml"], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
try:
    stdout, _ = proc.communicate(timeout=2)
    print("FAILED TO START")
    print(stdout.decode()[-1000:])
except subprocess.TimeoutExpired:
    proc.kill()
    print("Started successfully (timed out)")
