#!/usr/bin/env python3
import logging
from pathlib import Path

import blackboxprotobuf
import typer

app = typer.Typer()


@app.command()
def main(path: Path):
    message, typedef = blackboxprotobuf.protobuf_to_json(path.read_bytes())
    print(message)


if __name__ == "__main__":
    logging.basicConfig(format="%(levelname)s:%(message)s", level=logging.INFO)
    app()
