# Build database

Convert csv into a sqlite database
```bash
cd db
# the repeat flag control the number of loading
python load_csv_to_sqlite.py --dir csv/ --db data.db --repeat 20
cd ..
```

# gRPC backend

## Generate proto

```bash
cd grpc
python -m grpc_tools.protoc --proto_path=. sample_api.proto --python_out=. --grpc_python_out=.
cd ..
```

## Run gRPC backend server

```
python grpc/grpc_server.py
```

In another terminal, run smoke test to see if the server is running correctly
```
python grpc/smoke_test.py
```