import typer
import uvicorn

app = typer.Typer()

@app.command()
def start_server():
  uvicorn.run("src.restapi.app:app", host="127.0.0.1", port=8001, reload=True)


@app.command()
def ingest():
  print(f"Hello cli")    

if __name__ == "__main__":
  app()