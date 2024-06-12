## JSON/HTTP commit log service

### To produce a log
```bash
curl -X POST localhost:3000 -d '{"record": {"value":"TGV0J3MgR28gIzEK"}}'
```

### To consume a log
```bash
curl -X GET localhost:3000 -d '{"offset": 2}'
```
