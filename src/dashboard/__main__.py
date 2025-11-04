"""Command line entry point for the forward testing dashboard."""

from __future__ import annotations

import argparse

from .app import run


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the forward testing dashboard")
    parser.add_argument("--host", default="0.0.0.0", help="Host interface to bind (default: 0.0.0.0)")
    parser.add_argument("--port", type=int, default=8050, help="Port to serve the dashboard on")
    parser.add_argument("--debug", action="store_true", help="Run Dash in debug mode")
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    run(debug=args.debug, port=args.port, host=args.host)


if __name__ == "__main__":
    main()


