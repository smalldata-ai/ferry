import grpc
from ferry.src.grpc.protos import ferry_pb2, ferry_pb2_grpc

channel = grpc.insecure_channel("localhost:50051")
stub = ferry_pb2_grpc.FerryServiceStub(channel)

request = ferry_pb2.ObservabilityRequest(identity="test_kafka_268")
response = stub.GetObservability(request)

# print("Status:", response.status)
print("Metrics:", response.metrics)
