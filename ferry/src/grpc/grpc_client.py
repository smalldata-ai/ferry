import grpc
import ferry.src.grpc.ferry_pb2 as ferry_pb2
import ferry.src.grpc.ferry_pb2_grpc as ferry_pb2_grpc

def run():
    # Connect to the gRPC server
    channel = grpc.insecure_channel('localhost:50051')  # Ensure this matches your server port
    
    stub = ferry_pb2_grpc.DataLoaderStub(channel)  # Use DataLoaderStub instead
    
    # Create a request based on the provided input structure
    request = ferry_pb2.LoadDataRequest(
        source_uri="duckdb:///stroke.duckdb",
        destination_uri="duckdb:///destination1.duckdb",
        source_table_name="stroke",
        destination_table_name="my_table",
        dataset_name="my_dataset"
    )
    
    # Make the request to the gRPC server
    try:
        response = stub.LoadData(request)
        print(f"Response from server: {response}")
    except grpc.RpcError as e:
        print(f"gRPC error: {e.code()} - {e.details()}")

if __name__ == '__main__':
    run()
