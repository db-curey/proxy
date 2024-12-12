# Curey-Proxy

Curey-Proxy is a proxy server for Curey.

## Build

```bash
docker compose up -d
```

## Run

connect & get token
```
http://127.0.0.1:3000/connect?host=db&user=postgres&password=postgres&dbname=postgres
```

query
```
curl 'http://127.0.0.1:3000/query' -X POST -H 'User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:134.0) Gecko/20100101 Firefox/134.0' -H 'Accept: text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8' -H 'Accept-Language: zh-TW,zh-HK;q=0.8,zh-CN;q=0.6,en-US;q=0.4,en;q=0.2' -H 'Accept-Encoding: gzip, deflate, br, zstd' -H 'Connection: keep-alive' -H 'Upgrade-Insecure-Requests: 1' -H 'Sec-Fetch-Dest: document' -H 'Sec-Fetch-Mode: navigate' -H 'Sec-Fetch-Site: cross-site' -H 'Sec-Fetch-User: ?1' -H 'Priority: u=0, i' -H 'Origin: null' -H 'Pragma: no-cache' -H 'Cache-Control: no-cache' --data-raw '{"connection_id":1,"token":"421b7f8f-6a82-4c5f-af83-450261cc3e5c","sql":"select * from public.test", "args":[]}'
```