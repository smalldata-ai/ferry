import json
import os
from typing import Optional
import typer
import uvicorn
import yaml
from ferry.src.data_models.ingest_model import IngestModel, ResourceConfig
from ferry.src.data_models.response_models import LoadStatus
from ferry.src.pipeline_builder import PipelineBuilder
from ferry.src.security import SecretsManager

app = typer.Typer()
generate_app = typer.Typer()
app.add_typer(generate_app, name="generate", help="Generate-related commands")
show_app = typer.Typer()
app.add_typer(show_app, name="show", help="Show-related commands")

SECURE_MODE = False

@app.command()
def serve(
    host: str = typer.Option("127.0.0.1", help="Host to run the server on"),
    port: int = typer.Option(8001, help="Port to run the server on"),
    reload: bool = typer.Option(True, help="Enable auto-reload for development"),
    secure: bool = typer.Option(False, help="Enable HMAC authentication")
):
    """Start the FastAPI server for Ferry"""
    global SECURE_MODE
    SECURE_MODE = secure
    if secure:
        if not SecretsManager.credentials_exist():
            typer.echo("Error: No credentials found. Run 'ferry generate secrets' first.")
            raise typer.Exit(1)
        typer.echo(f"Starting Ferry server on {host}:{port} with HMAC authentication")
    else:
        typer.echo(f"Starting Ferry server on {host}:{port}")
    uvicorn.run("ferry.src.restapi.app:app", host=host, port=port, reload=reload)

@generate_app.command("secrets")
def generate_secrets():
    """Generate new client credentials"""
    try:
        creds = SecretsManager.generate_secret()
        typer.echo("New credentials generated:")
        typer.echo(f"Client ID: {creds.client_id}")
        typer.echo(f"Client Secret: {creds.client_secret}")
        typer.echo("Store these securely")
    except Exception as e:
        typer.echo(f"Error generating secrets: {str(e)}")
        raise typer.Exit(1)

@show_app.command("secrets")
def show_secrets():
    """Show current client credentials"""
    try:
        creds = SecretsManager.get_credentials()
        typer.echo("Current credentials:")
        typer.echo(f"Client ID: {creds.client_id}")
        typer.echo(f"Client Secret: {creds.client_secret}")
    except ValueError:
        typer.echo("No credentials found. Run 'ferry generate secrets' first.")
        raise typer.Exit(1)
    except Exception as e:
        typer.echo(f"Error retrieving secrets: {str(e)}")
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
            typer.echo("Error: Either --resources-file or --resources-json must be provided", err=True)
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

        typer.echo(f"Starting ingestion with identity: {identity}")
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
                typer.echo(f"Warning: Could not read schema file - {str(e)}", err=True)

        response = {
            "status": LoadStatus.SUCCESS.value,
            "message": "Data Ingestion is completed successfully",
            "pipeline_name": pipeline.get_name(),
            "schema_version_hash": schema_version_hash
        }
        
        typer.echo(json.dumps(response, indent=2))
        return 0
        
    except json.JSONDecodeError as e:
        typer.echo(f"Error: Invalid JSON format - {str(e)}", err=True)
        raise typer.Exit(1)
    except Exception as e:
        error_response = {
            "status": "error",
            "message": f"An error occurred during ingestion: {str(e)}"
        }
        typer.echo(json.dumps(error_response, indent=2), err=True)
        raise typer.Exit(1)

@app.command()
def version():
    """Display the Ferry version"""
    typer.echo("Ferry version 0.1.0")

if __name__ == "__main__":
    app()