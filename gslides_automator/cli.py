"""Command-line interface for gslides_automator."""

from __future__ import annotations

import argparse
import sys
from typing import Callable


def _run_generate(args: argparse.Namespace) -> int:
    """Entrypoint for the `generate` subcommand."""
    from .generate import generate, get_oauth_credentials, resolve_layout

    creds = get_oauth_credentials(
        service_account_credentials=args.service_account_credentials
    )
    layout = resolve_layout(args.shared_drive_url, creds)
    generate(
        creds=creds,
        layout=layout,
    )
    return 0


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="gslides_automator",
        description="Utilities for generating data and reports from Google Drive and Sheets.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    generate_parser = subparsers.add_parser(
        "generate",
        help="Generate L1-Merged, L2-Slides, and L3-PDF from entities.csv.",
    )
    generate_parser.add_argument(
        "--shared-drive-url",
        required=True,
        help="Shared Drive root URL or ID that contains L0/L1/L2/L3 data and templates.",
    )
    generate_parser.add_argument(
        "--service-account-credentials",
        default=None,
        help="Path to the service account JSON key file. Defaults to service-account-credentials.json in the project root.",
    )
    generate_parser.set_defaults(func=_run_generate)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    handler: Callable[[argparse.Namespace], int] | None = getattr(args, "func", None)
    if handler is None:
        parser.print_help()
        return 1

    try:
        return handler(args)
    except Exception as exc:  # pragma: no cover - CLI guardrail
        print(f"Error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
