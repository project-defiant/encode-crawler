"""Crawler implementation for downloading the E2G datasets."""

import typer
from typing import Annotated
import os
from pathlib import Path
from loguru import logger
import aiofiles
import aiohttp
from aiohttp.client_exceptions import ClientError
import asyncio

import tqdm
import polars as pl

cli = typer.Typer(no_args_is_help=True)


# Save a single file
async def download_file(
    session: aiohttp.ClientSession,
    url: str,
    semaphore: asyncio.Semaphore,
    output_dir: str,
    file_size: int,
    file_name: str,
    retries=3,
):
    filename = Path(output_dir) / file_name

    if filename.exists():
        if os.stat(filename).st_size == file_size:
            return  # skip if already downloaded
        else:
            logger.warning(
                f"File {filename} exists but size does not match. Redownloading."
            )
    else:
        logger.info(f"Downloading {url} to {filename}")

    async with semaphore:
        for attempt in range(retries):
            try:
                async with session.get(
                    url, timeout=aiohttp.ClientTimeout(total=60)
                ) as resp:
                    if resp.status == 200:
                        async with aiofiles.open(filename, mode="wb") as f:
                            await f.write(await resp.read())
                        return
                    else:
                        raise ClientError(f"Status {resp.status}")
            except Exception as e:
                if attempt == retries - 1:
                    print(f"Failed to download {url}: {e}")
                await asyncio.sleep(1 + attempt * 2)  # backoff


async def download_all(transfer_objects, output_dir, max_concurrent=20):
    semaphore = asyncio.Semaphore(max_concurrent)

    async with aiohttp.ClientSession() as session:
        tasks = [
            download_file(session, to["url"], semaphore, output_dir, to["size"], name)
            for name, to in transfer_objects.items()
        ]
        for f in tqdm.tqdm(
            asyncio.as_completed(tasks), total=len(tasks), desc="Downloading"
        ):
            await f  # run in progress


@cli.command()
def main(
    input: Annotated[str, typer.Option(help="Input manifest file path")],
    output_dir: Annotated[
        str, typer.Option(help="Output directory for downloaded datasets")
    ],
    base_uri: Annotated[
        str, typer.Option(help="Base URI for the datasets")
    ] = "https://www.encodeproject.org",
):
    """Download the E2G datasets."""
    # Check if the input file exists

    pl.Config.set_fmt_str_lengths(100)
    input_path = Path(input)
    if not input_path.exists():
        logger.error(f"Input file {input} does not exist.")
        raise typer.Exit(code=1)

    # Check if the output directory exists, create if not
    output_path = Path(output_dir)
    if not output_path.exists():
        logger.warning(f"Output directory {output_dir} does not exist. Creating it.")
        output_path.mkdir(parents=True, exist_ok=True)

    # Read the input manifest file
    df = pl.read_csv(input_path, skip_rows=1, separator="\t", has_header=True)
    logger.info(f"Read {len(df)} entries from the input manifest file.")
    if df.is_empty():
        typer.echo("Input manifest file is empty.")
        raise typer.Exit(code=1)

    # Show the first few rows of the DataFrame
    logger.info("First few rows of the DataFrame:")
    logger.info(df.head())

    uri_file_map = df.select(
        pl.col("Download URL").str.split("/").list.last().alias("FILE_NAME"),
        pl.concat_str(pl.lit(base_uri), pl.col("Download URL")).alias("URI"),
        pl.col("File size"),
    )
    uri_mapping = {v[0]: {"url": v[1], "size": v[2]} for v in uri_file_map.to_numpy()}

    logger.info(f"Total URLs to download: {len(uri_mapping)}")
    if not uri_mapping:
        logger.error("No URLs to download.")
        raise typer.Exit(code=1)
    logger.info("Starting download...")
    asyncio.run(download_all(uri_mapping, output_dir))
