#!/usr/bin/env python3
"""Minimal dependency-free stdio MCP allowlist smoke check for RepoWise."""

from __future__ import annotations

import json
import selectors
import subprocess
import sys
import time
from pathlib import Path


def fail(message: str, process: subprocess.Popen[str] | None = None) -> "NoReturn":
    if process is not None:
        process.terminate()
        try:
            process.wait(timeout=2)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=2)
    raise SystemExit(message)


def receive(
    process: subprocess.Popen[str],
    selector: selectors.BaseSelector,
    request_id: int,
    timeout_seconds: int = 10,
) -> dict[str, object]:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if process.poll() is not None:
            stderr = process.stderr.read() if process.stderr is not None else ""
            fail(f"RepoWise MCP exited before response {request_id}: {stderr.strip()}")
        if not selector.select(timeout=max(0, deadline - time.monotonic())):
            continue
        assert process.stdout is not None
        line = process.stdout.readline()
        if not line:
            continue
        try:
            message = json.loads(line)
        except json.JSONDecodeError:
            continue
        if message.get("id") == request_id:
            return message
    fail(f"timed out waiting for RepoWise MCP response {request_id}", process)


def send(process: subprocess.Popen[str], payload: dict[str, object]) -> None:
    assert process.stdin is not None
    process.stdin.write(json.dumps(payload, separators=(",", ":")) + "\n")
    process.stdin.flush()


def main() -> int:
    if len(sys.argv) != 4:
        raise SystemExit("usage: mcp_smoke.py REPOWISE_BIN REPO_ROOT TOOL_ALLOWLIST")
    repowise_bin, repo_root, allowlist_text = sys.argv[1:]
    if Path(repo_root).resolve() != Path.cwd().resolve():
        raise SystemExit("MCP smoke must run from the Sessionless repository root")

    expected = set(allowlist_text.split(","))
    process = subprocess.Popen(
        [
            repowise_bin,
            "mcp",
            repo_root,
            "--transport",
            "stdio",
            "--tools",
            allowlist_text,
        ],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
        bufsize=1,
    )
    assert process.stdout is not None
    selector = selectors.DefaultSelector()
    selector.register(process.stdout, selectors.EVENT_READ)

    send(
        process,
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2025-06-18",
                "capabilities": {},
                "clientInfo": {"name": "sessionless-repowise-smoke", "version": "1"},
            },
        },
    )
    initialized = receive(process, selector, 1)
    if "error" in initialized:
        fail(f"RepoWise MCP initialize failed: {initialized['error']}", process)
    send(process, {"jsonrpc": "2.0", "method": "notifications/initialized", "params": {}})
    send(process, {"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}})
    listed = receive(process, selector, 2)
    if "error" in listed:
        fail(f"RepoWise MCP tools/list failed: {listed['error']}", process)

    result = listed.get("result")
    if not isinstance(result, dict) or not isinstance(result.get("tools"), list):
        fail("RepoWise MCP returned an invalid tools/list payload", process)
    actual = {
        tool.get("name")
        for tool in result["tools"]
        if isinstance(tool, dict) and isinstance(tool.get("name"), str)
    }
    if actual != expected:
        fail(
            "RepoWise MCP tool exposure mismatch: "
            f"expected={sorted(expected)} actual={sorted(actual)}",
            process,
        )

    calls: list[tuple[str, dict[str, object]]] = [
        ("get_overview", {}),
        ("get_context", {"targets": ["README.md"]}),
        ("get_change_risk", {"revspec": "HEAD", "baseline": 0}),
        ("get_health", {"limit": 1}),
        ("get_dead_code", {"limit": 1}),
    ]
    for request_id, (tool_name, arguments) in enumerate(calls, start=3):
        send(
            process,
            {
                "jsonrpc": "2.0",
                "id": request_id,
                "method": "tools/call",
                "params": {"name": tool_name, "arguments": arguments},
            },
        )
        called = receive(process, selector, request_id, timeout_seconds=60)
        if "error" in called:
            fail(f"RepoWise MCP {tool_name} call failed: {called['error']}", process)
        call_result = called.get("result")
        if not isinstance(call_result, dict) or call_result.get("isError") is True:
            fail(f"RepoWise MCP {tool_name} returned an unsuccessful result", process)

    process.terminate()
    try:
        process.wait(timeout=2)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=2)
    print(
        json.dumps(
            {"ok": True, "tools": sorted(actual), "calls": [name for name, _ in calls]},
            separators=(",", ":"),
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
