import subprocess
conf = """<clickhouse>
<logger><level>trace</level><console>true</console></logger>
<http_port>12345</http_port>
<listen_host>127.0.0.1</listen_host>
<path>./tmp-ch/data/</path>
<tmp_path>./tmp-ch/tmp/</tmp_path>
<user_files_path>./tmp-ch/user_files/</user_files_path>
<users_config>./tmp-ch/users.xml</users_config>
<mark_cache_size>268435456</mark_cache_size>
<max_server_memory_usage>1073741824</max_server_memory_usage>
</clickhouse>"""
users = "<clickhouse><profiles><default><max_threads>1</max_threads><async_insert>1</async_insert><wait_for_async_insert>0</wait_for_async_insert></default></profiles><users><default><password></password><networks><ip>127.0.0.1</ip><ip>::1</ip></networks><profile>default</profile><quota>default</quota><access_management>1</access_management></default></users><quotas><default/></quotas></clickhouse>"
import os
os.makedirs("./tmp-ch/data", exist_ok=True)
os.makedirs("./tmp-ch/tmp", exist_ok=True)
os.makedirs("./tmp-ch/user_files", exist_ok=True)
open("./tmp-ch/config.xml", "w").write(conf)
open("./tmp-ch/users.xml", "w").write(users)

proc = subprocess.Popen(["/Users/ubuntu/.shared-clickhouse-bin/clickhouse", "server", "--config-file", "./tmp-ch/config.xml"], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
try:
    stdout, _ = proc.communicate(timeout=5)
    print(stdout.decode()[-2000:])
except subprocess.TimeoutExpired:
    proc.kill()
    stdout, _ = proc.communicate()
    print("timeout, it ran successfully")
    print(stdout.decode()[-1000:])
