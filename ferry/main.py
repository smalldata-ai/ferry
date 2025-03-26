import typer
import uvicorn
from typing import Optional

app = typer.Typer()

@app.command()
def serve(
    host: str = typer.Option("127.0.0.1", help="Host to run the server on"),
    port: int = typer.Option(8001, help="Port to run the server on"),
    reload: bool = typer.Option(True, help="Enable auto-reload for development")
):
    """Start the FastAPI server for Ferry"""
    typer.echo(f"Starting Ferry server on {host}:{port}")
    uvicorn.run("ferry.src.restapi.app:app", host=host, port=port, reload=reload)

@app.command()
def ingest():
    """Run the ingest command"""
    typer.echo("Hello CLI")

@app.command()
def version():
    """Display the Ferry version"""
    typer.echo("Ferry version 0.1.0")

if __name__ == "__main__":
    app()