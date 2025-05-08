# Build database

Convert csv into a sqlite database
```bash
cd db
python load_csv_to_sqlite.py --dir csv/ --db data.db
cd ..
```

# Generate proto

```bash
cd grpc
python -m grpc_tools.protoc --proto_path=. sample_api.proto --python_out=. --grpc_python_out=.
cd ..
```

Run gRPC backend server

```
python grpc/server.py
```

In another terminal, Run gRPC test client to see if the server 
```
python grpc/test_client.py
```