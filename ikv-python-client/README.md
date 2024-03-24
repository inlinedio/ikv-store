Readme: See https://github.com/inlinedio/ikv-store


Generating python code from proto:

$> python3 -m pip install grpcio-tools

$> pwd
/Users/pushkar/projects/ikv-store/ikv-python-client/ikv-py

$> python3 -m grpc_tools.protoc -I../../ikv-cloud/src/main/proto --python_out=./schemas --pyi_out=./schemas --grpc_python_out=./schemas ../../ikv-cloud/src/main/proto/*.proto
