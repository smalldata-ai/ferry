import typer
import uvicorn
from ferry.src.security import SecretsManager

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
def ingest():
    """Run the ingest command"""
    typer.echo("Hello CLI")

@app.command()
def version():
    """Display the Ferry version"""
    typer.echo("Ferry version 0.1.0")

if __name__ == "__main__":
    app()