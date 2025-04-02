import json
import os
from typing import Optional
import typer
import uvicorn
import yaml
from rich.console import Console
from rich.theme import Theme
from rich.panel import Panel
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, TextColumn
from ferry.src.data_models.ingest_model import IngestModel, ResourceConfig
from ferry.src.data_models.response_models import LoadStatus
from ferry.src.pipeline_builder import PipelineBuilder
from ferry.src.security import SecretsManager
import subprocess


custom_theme = Theme({
    "info": "dim cyan",
    "warning": "magenta",
    "danger": "bold red",
    "success": "bold green",
    "header": "bold white on blue",
    "highlight": "bold yellow",
})


console = Console(theme=custom_theme)

app = typer.Typer()
generate_app = typer.Typer()
app.add_typer(generate_app, name="generate", help="Generate-related commands")
show_app = typer.Typer()
app.add_typer(show_app, name="show", help="Show-related commands")

SECURE_MODE = False

@app.command()
def serve(
    host: str = typer.Option("0.0.0.0", help="Host to run the server on"),
    port: int = typer.Option(8001, help="Port to run the server on"),
    reload: bool = typer.Option(True, help="Enable auto-reload for development"),
    secure: bool = typer.Option(False, help="Enable HMAC authentication")
):
    """Start the FastAPI server for Ferry"""
    global SECURE_MODE
    SECURE_MODE = secure
    
    title = "[header]Ferry Server[/header]"
    if secure:
        if not SecretsManager.credentials_exist():
            console.print(Panel(
                "[danger]Error: No credentials found. Run 'ferry generate secrets' first.",
                title=title,
                border_style="danger"
            ))
            raise typer.Exit(1)
        
        console.print(Panel(
            f"[success]Starting Ferry server on {host}:{port} with HMAC authentication",
            title=title,
            border_style="success"
        ))
    else:
        console.print(Panel(
            f"[success]Starting Ferry server on {host}:{port}",
            title=title,
            border_style="success"
        ))
    uvicorn.run("ferry.src.restapi.app:app", host=host, port=port, reload=reload)

@app.command()
def serve_grpc(
    port: int = typer.Option(50051, help="Port to run the gRPC server on"),
    secure: bool = typer.Option(False, help="Enable HMAC authentication for gRPC")
):
    """Start the gRPC server for Ferry"""
    cmd = ["python", "ferry/src/grpc/grpc_server.py", "--port", str(port)]
    if secure:
        cmd.append("--secure")
    
    typer.echo(f"Starting Ferry gRPC server on port {port} {'with HMAC authentication' if secure else ''}")
    subprocess.run(cmd)

@generate_app.command("secrets")
def generate_secrets():
    """Generate new client credentials"""
    try:
        creds = SecretsManager.generate_secret()
        
        table = Table(title="[header]New Credentials Generated[/header]", border_style="success")
        table.add_column("Field", style="highlight")
        table.add_column("Value", style="success")
        
        table.add_row("Client ID", creds.client_id)
        table.add_row("Client Secret", creds.client_secret)
        
        console.print(table)
        console.print("[warning]Store these securely - they cannot be recovered if lost![/warning]")
    except Exception as e:
        console.print(Panel(
            f"[danger]Error generating secrets: {str(e)}",
            title="[header]Error[/header]",
            border_style="danger"
        ))
        raise typer.Exit(1)

@show_app.command("secrets")
def show_secrets():
    """Show current client credentials"""
    try:
        creds = SecretsManager.get_credentials()
        
        table = Table(title="[header]Current Credentials[/header]", border_style="info")
        table.add_column("Field", style="highlight")
        table.add_column("Value", style="info")
        
        table.add_row("Client ID", creds.client_id)
        table.add_row("Client Secret", creds.client_secret)
        
        console.print(table)
    except ValueError:
        console.print(Panel(
            "[danger]No credentials found. Run 'ferry generate secrets' first.",
            title="[header]Error[/header]",
            border_style="danger"
        ))
        raise typer.Exit(1)
    except Exception as e:
        console.print(Panel(
            f"[danger]Error retrieving secrets: {str(e)}",
            title="[header]Error[/header]",
            border_style="danger"
        ))
        raise typer.Exit(1)

@app.command()
def ingest(
    identity: str = typer.Option(..., "--identity", help="Identity for the run"),
    source_uri: str = typer.Option(..., "--source-uri", help="Source DB URI"),
    destination_uri: str = typer.Option(..., "--destination-uri", help="Destination DB URI"),
    dataset_name: Optional[str] = typer.Option(None, "--dataset-name", help="Schema name"),
    resources_file: Optional[str] = typer.Option(
        None, 
        "--resources-file", 
        help="Path to a JSON file containing the resources array",
        exists=True,
        readable=True,
        dir_okay=False
    ),
    resources_json: Optional[str] = typer.Option(
        None,
        "--resources-json",
        help="JSON string containing the resources array"
    )
):
    """Run data ingestion between source and destination databases"""
    try:
        if not resources_file and not resources_json:
            console.print(Panel(
                "[danger]Error: Either --resources-file or --resources-json must be provided",
                title="[header]Error[/header]",
                border_style="danger"
            ), style="danger")
            raise typer.Exit(1)

        if resources_file:
            with open(resources_file) as f:
                resources_data = json.load(f)
        else:
            resources_data = json.loads(resources_json)

        resources = [ResourceConfig(**resource) for resource in resources_data]

        ingest_model = IngestModel(
            identity=identity,
            source_uri=source_uri,
            destination_uri=destination_uri,
            dataset_name=dataset_name,
            resources=resources
        )

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            transient=True,
        ) as progress:
            progress.add_task(description=f"[info]Starting ingestion with identity: {identity}", total=None)
            
            pipeline = PipelineBuilder(ingest_model).build()
            pipeline.run()

        schema_version_hash = None
        pipeline_schema_path = os.path.join(".schemas", f"{ingest_model.identity}.schema.yaml")
        
        if os.path.exists(pipeline_schema_path):
            try:
                with open(pipeline_schema_path, "r") as f:
                    schema_data = yaml.safe_load(f)
                schema_version_hash = schema_data.get("version_hash", "")
            except Exception as e:
                console.print(f"[warning]Warning: Could not read schema file - {str(e)}[/warning]")

        response = {
            "status": LoadStatus.SUCCESS.value,
            "message": "Data Ingestion is completed successfully",
            "pipeline_name": pipeline.get_name(),
            "schema_version_hash": schema_version_hash
        }
        
        console.print(Panel(
            json.dumps(response, indent=2),
            title="[header]Ingestion Complete[/header]",
            border_style="success"
        ))
        return 0
        
    except json.JSONDecodeError as e:
        console.print(Panel(
            f"[danger]Error: Invalid JSON format - {str(e)}",
            title="[header]Error[/header]",
            border_style="danger"
        ))
        raise typer.Exit(1)
    except Exception as e:
        error_response = {
            "status": "error",
            "message": f"An error occurred during ingestion: {str(e)}"
        }
        console.print(Panel(
            json.dumps(error_response, indent=2),
            title="[header]Ingestion Failed[/header]",
            border_style="danger"
        ))
        raise typer.Exit(1)

@app.command()
def version():
    """Display the Ferry version"""
    console.print(Panel(
        "[highlight]Ferry version 0.1.0[/highlight]",
        title="[header]Version[/header]",
        border_style="highlight"
    ))

if __name__ == "__main__":
    app()