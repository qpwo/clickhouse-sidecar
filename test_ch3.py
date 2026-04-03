import subprocess, os
conf = """<clickhouse>
<logger><level>trace</level><console>true</console></logger>
<http_port>12345</http_port>
<tcp_port>12346</tcp_port>
<listen_host>127.0.0.1</listen_host>
<path>./tmp-ch3/data/</path>
<tmp_path>./tmp-ch3/tmp/</tmp_path>
<user_files_path>./tmp-ch3/user_files/</user_files_path>
<users_config>./tmp-ch3/users.xml</users_config>
</clickhouse>"""
users = "<clickhouse><profiles><default/></profiles><users><default><password></password><networks><ip>127.0.0.1</ip><ip>::1</ip></networks><profile>default</profile><quota>default</quota><access_management>1</access_management></default></users><quotas><default/></quotas></clickhouse>"
os.makedirs("./tmp-ch3/data", exist_ok=True)
os.makedirs("./tmp-ch3/tmp", exist_ok=True)
os.makedirs("./tmp-ch3/user_files", exist_ok=True)
open("./tmp-ch3/config.xml", "w").write(conf)
open("./tmp-ch3/users.xml", "w").write(users)

proc = subprocess.Popen(["/Users/ubuntu/.shared-clickhouse-bin/clickhouse", "server", "--config-file", "./tmp-ch3/config.xml"], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
try:
    stdout, _ = proc.communicate(timeout=5)
    print("FAILED TO START")
    print(stdout.decode()[-1000:])
except subprocess.TimeoutExpired:
    proc.kill()
    print("Started successfully (timed out)")
