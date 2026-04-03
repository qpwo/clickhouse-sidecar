import subprocess, os, time
conf = """<clickhouse>
<logger><level>trace</level><console>true</console></logger>
<listen_host>127.0.0.1</listen_host>
<tcp_port></tcp_port>
<http_port></http_port>
<mysql_port></mysql_port>
<postgresql_port></postgresql_port>
<interserver_http_port></interserver_http_port>
</clickhouse>"""
# Let's try adding <http_port></http_port> and see what it takes. Wait, empty string breaks it. Let's just omit them.
conf = """<clickhouse>
<logger><level>trace</level><console>true</console></logger>
<listen_host>127.0.0.1</listen_host>
<path>./tmp-ch5/data/</path>
</clickhouse>"""
os.makedirs("./tmp-ch5/data", exist_ok=True)
open("./tmp-ch5/config.xml", "w").write(conf)
