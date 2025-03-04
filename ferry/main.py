import typer
import uvicorn
import subprocess

app = typer.Typer()

@app.command()
def start_server():
    """Starts the FastAPI server"""
    uvicorn.run("src.restapi.app:app", host="127.0.0.1", port=8001, reload=True)

@app.command()
def start_grpc_server():
    """Starts the gRPC server"""
    subprocess.run(["python", "ferry/src/grpc/grpc_server.py"])

@app.command()
def ingest():
    """Example CLI command"""
    print(f"Hello CLI")

if __name__ == "__main__":
    app()
