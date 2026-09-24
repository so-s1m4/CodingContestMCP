"""Transfer local files through the existing MCP tools without printing their contents."""

import argparse
import asyncio
import base64
import getpass
import hashlib
import json
import os
import re
import sys
import tempfile
from pathlib import Path
from urllib.parse import urlencode, urlsplit

import httpx
from mcp import ClientSession
from mcp.shared.exceptions import McpError


async def call(session, name, **arguments):
    response = await session.call_tool(name, arguments)
    body = response.structuredContent
    if response.isError or not isinstance(body, dict) or body.get("ok") is not True:
        raise ValueError(json.dumps(body or {"error": "MCP tool failed"}))
    return body["data"]


async def upload(session, path: Path, limit: int):
    with path.open("rb") as source:
        payload = source.read(limit + 1)
    if len(payload) > limit:
        raise ValueError("File exceeds --max-bytes")
    return await call(
        session,
        "upload_artifact",
        data_base64=base64.b64encode(payload).decode(),
        filename=path.name,
    )


async def upload_http(http, url, path: Path, limit: int):
    size = path.stat().st_size
    if size > limit:
        raise ValueError("File exceeds --max-bytes")

    async def chunks():
        with path.open("rb") as source:
            while chunk := await asyncio.to_thread(source.read, 262144):
                yield chunk

    response = await http.post(
        f"{url.rstrip('/')}/artifacts/upload?{urlencode({'filename': path.name})}",
        content=chunks(),
        headers={"Content-Type": "application/octet-stream"},
    )
    response.raise_for_status()
    metadata = response.json()
    if metadata.get("bytes") != size:
        raise ValueError("Upload size did not match the local file")
    return metadata


async def transfer(args, cookie):
    async with httpx.AsyncClient(
        headers={"X-CCC-Session": cookie},
        timeout=60,
        follow_redirects=False,
    ) as http:
        if args.command == "download":
            return await download_http(
                http, args.url, args.artifact_id, args.path, args.max_bytes
            )
        return await upload_http(http, args.url, args.path, args.max_bytes)


async def download_http(http, url, artifact, path, limit):
    if not re.fullmatch(r"[a-f0-9]{32}", artifact):
        raise ValueError("Invalid artifact_id")
    if path.exists() or path.is_symlink():
        raise ValueError("Destination already exists; choose a new path")
    digest, size = hashlib.sha256(), 0
    async with http.stream(
        "GET", f"{url.rstrip('/')}/artifacts/{artifact}"
    ) as response:
        response.raise_for_status()
        if int(response.headers.get("content-length", "0")) > limit:
            raise ValueError("File exceeds --max-bytes")
        with tempfile.NamedTemporaryFile(dir=path.parent) as target:
            async for chunk in response.aiter_bytes(262144):
                size += len(chunk)
                if size > limit:
                    raise ValueError("File exceeds --max-bytes")
                target.write(chunk)
                digest.update(chunk)
            target.flush()
            os.link(target.name, path)
    return {"path": str(path), "bytes": size, "sha256": digest.hexdigest()}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", required=True, help="MCP endpoint URL")
    parser.add_argument("--max-bytes", type=int, default=64 * 1024 * 1024)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser(
        "upload", help="Upload a local file; print its artifact_id"
    ).add_argument("path", type=Path)
    get = commands.add_parser("download", help="Save an artifact to a new local file")
    get.add_argument("artifact_id")
    get.add_argument("path", type=Path)
    args = parser.parse_args()
    url = urlsplit(args.url)
    if (
        not url.hostname
        or url.username
        or url.password
        or url.query
        or url.fragment
        or not (
            url.scheme == "https"
            or (
                url.scheme == "http"
                and url.hostname in ("localhost", "127.0.0.1", "::1")
            )
        )
        or args.max_bytes <= 0
    ):
        parser.error(
            "Use HTTPS (HTTP only on localhost), no URL credentials/query, and positive --max-bytes"
        )
    cookie = os.getenv("CCC_SESSION")
    if not cookie:
        if not sys.stdin.isatty():
            parser.error(
                "Set CCC_SESSION locally or run interactively to enter the cookie"
            )
        cookie = getpass.getpass("CCC SESSION cookie: ")
    if not re.fullmatch(r"[A-Za-z0-9_+/=%.-]{16,4096}", cookie):
        parser.error("Supply only the SESSION cookie value, without SESSION=")
    try:
        print(json.dumps(asyncio.run(transfer(args, cookie))))
    except (ValueError, OSError, httpx.HTTPError, McpError, ExceptionGroup) as error:
        while isinstance(error, BaseExceptionGroup) and error.exceptions:
            error = error.exceptions[0]
        print(
            f"Transfer failed: {str(error).replace(cookie, '[redacted]')}",
            file=sys.stderr,
        )
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
