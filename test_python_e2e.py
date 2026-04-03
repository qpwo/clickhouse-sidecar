"""Hard e2e test for the Python sidecar: settings, read-after-write, SSE, and clean shutdown."""
import http.client
import json
import os
import signal
import sys
import time
import urllib.parse

sys.path.append('python-pkg')
import clickhouse_sidecar

def main():
    data_dir = os.path.abspath('.test-python-e2e-' + str(int(time.time() * 1000)))
    client = clickhouse_sidecar.get_client(data_dir=data_dir)
    print('sidecar url', client.sidecar_url)
    assert fetch_text(client.sidecar_url + '/ping').strip() == 'Ok.'
    client.command('DROP TABLE IF EXISTS events')
    client.command('CREATE TABLE events (id UInt32, level String, ts DateTime64(3)) ENGINE = MergeTree ORDER BY id')
    assert_settings(client, data_dir)
    check_read_after_write(client)
    check_streaming(client)
    scalar(client, 'SELECT count() FROM events')
    check_health(client.sidecar_url)
    state = json.load(open(os.path.join(data_dir, 'sidecar-state.json')))
    client.close()
    wait_for_dead(state['pid'], 10)
    wait_for_dead(state['clickhousePid'], 10)
    print('python e2e passed')

def assert_settings(client, data_dir):
    max_threads = scalar(client, "SELECT value FROM system.settings WHERE name='max_threads'")
    wait_async = scalar(client, "SELECT value FROM system.settings WHERE name='wait_for_async_insert'")
    async_insert = scalar(client, "SELECT value FROM system.settings WHERE name='async_insert'")
    if str(max_threads) != '1':
        raise RuntimeError('max_threads != 1: ' + str(max_threads))
    if str(wait_async) != '0':
        raise RuntimeError('wait_for_async_insert != 0: ' + str(wait_async))
    if str(async_insert) != '1':
        raise RuntimeError('async_insert != 1: ' + str(async_insert))
    config = open(os.path.join(data_dir, 'config.xml')).read()
    users = open(os.path.join(data_dir, 'users.xml')).read()
    if '<max_server_memory_usage>1073741824</max_server_memory_usage>' not in config:
        raise RuntimeError('missing max_server_memory_usage')
    if '<tcp_port>' in config:
        raise RuntimeError('tcp port enabled')
    if '<max_memory_usage>1073741824</max_memory_usage>' not in users:
        raise RuntimeError('missing max_memory_usage')

def check_read_after_write(client):
    for i in range(40):
        client.insert('events', [[i, 'py', now_string()]], column_names=['id', 'level', 'ts'])
        if (i + 1) % 10:
            continue
        t1 = time.time()
        n1 = int(scalar(client, 'SELECT count() FROM events'))
        dt1 = int((time.time() - t1) * 1000)
        t2 = time.time()
        n2 = int(scalar(client, 'SELECT count() FROM events'))
        dt2 = int((time.time() - t2) * 1000)
        print('after', i + 1, 'count', n1, 'first_read_ms', dt1, 'second_read_ms', dt2)
        if n1 != i + 1 or n2 != i + 1:
            raise RuntimeError('read-after-write failed')
        if dt2 > 500:
            raise RuntimeError('second read should be cheap, got ' + str(dt2))

def check_streaming(client):
    conn = http.client.HTTPConnection('127.0.0.1', client.sidecar_port, timeout=10)
    conn.putrequest('GET', '/_sidecar/events?table=events')
    conn.endheaders()
    res = conn.getresponse()
    if res.status != 200:
        raise RuntimeError('stream status != 200')
    post_json_each_row(
        client.sidecar_port,
        'INSERT INTO events FORMAT JSONEachRow',
        [{'id': 1000 + i, 'level': 'stream', 'ts': now_string()} for i in range(10)]
    )
    rows = read_sse_rows(res, 10, 5)
    ids = sorted(row['id'] for row in rows)
    if ids[0] != 1000 or ids[-1] != 1009:
        raise RuntimeError('bad stream ids ' + json.dumps(ids))
    conn.close()
    print('stream rows ok', len(rows))

def check_health(sidecar_url):
    text = fetch_text(sidecar_url + '/_sidecar/health')
    payload = json.loads(text)
    if payload['dirty']:
        raise RuntimeError('sidecar still dirty after final read')
    if not payload['ready']:
        raise RuntimeError('sidecar not ready')

def scalar(client, query):
    row = client.query(query).result_rows[0]
    return row[0]

def fetch_text(url):
    parts = url.split(':')
    host = parts[1].replace('//', '')
    port, path = parts[2].split('/', 1)
    conn = http.client.HTTPConnection(host, int(port), timeout=5)
    conn.request('GET', '/' + path)
    res = conn.getresponse()
    data = res.read().decode()
    conn.close()
    return data

def post_json_each_row(port, query, rows):
    body = '\n'.join(json.dumps(row) for row in rows) + '\n'
    conn = http.client.HTTPConnection('127.0.0.1', port, timeout=10)
    conn.request('POST', '/?query=' + urllib_quote(query), body=body.encode(), headers={'content-type': 'text/plain; charset=utf-8'})
    res = conn.getresponse()
    data = res.read().decode()
    conn.close()
    if res.status != 200:
        raise RuntimeError('insert failed ' + str(res.status) + ' ' + data)

def read_sse_rows(res, count, timeout_s):
    buf = ''
    rows = []
    deadline = time.time() + timeout_s
    while len(rows) < count:
        if time.time() > deadline:
            raise RuntimeError('stream timeout rows=' + str(len(rows)))
        chunk = res.fp.readline().decode()
        if not chunk:
            continue
        buf += chunk
        if not buf.endswith('\n\n'):
            continue
        block = buf.strip()
        buf = ''
        if block.startswith(':'):
            continue
        event_name = ''
        data = ''
        for line in block.splitlines():
            if line.startswith('event: '):
                event_name = line[7:]
            if line.startswith('data: '):
                data += line[6:]
        if event_name == 'row' and data:
            rows.append(json.loads(data))
    return rows

def urllib_quote(text):
    return urllib.parse.quote(text)

def now_string():
    return time.strftime('%Y-%m-%dT%H:%M:%S') + '.000Z'

def wait_for_dead(pid, timeout_s):
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        stat = get_pid_stat(pid)
        if not stat or 'Z' in stat:
            return
        time.sleep(0.1)
    raise RuntimeError('pid still alive ' + str(pid) + ' stat=' + repr(get_pid_stat(pid)))

def get_pid_stat(pid):
    try:
        return os.popen(f'ps -o stat= -p {pid}').read().strip()
    except Exception:
        return ''

if __name__ == '__main__':
    main()
