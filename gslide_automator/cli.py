"""Command-line interface for gslide_automator."""

from __future__ import annotations

import argparse
import sys
from typing import Callable


def _run_generate_data(args: argparse.Namespace) -> int:
    """Entrypoint for the `generate-data` subcommand."""
    from .generate_data import generate_data, get_oauth_credentials, resolve_layout

    creds = get_oauth_credentials(service_account_file=args.credentials)
    layout = resolve_layout(args.shared_drive_url, creds)
    generate_data(
        entities=args.entities,
        creds=creds,
        layout=layout,
    )
    return 0


def _run_generate_report(args: argparse.Namespace) -> int:
    """Entrypoint for the `generate-report` subcommand."""
    from .generate_report import generate_report, get_oauth_credentials, resolve_layout

    creds = get_oauth_credentials(service_account_file=args.credentials)
    layout = resolve_layout(args.shared_drive_url, creds)
    generate_report(
        entities=args.entities,
        creds=creds,
        layout=layout,
    )
    return 0


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="gslide_automator",
        description="Utilities for generating data and reports from Google Drive and Sheets.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    gen_parser = subparsers.add_parser(
        "generate-data",
        help="Generate L1 data from the shared drive layout.",
    )
    gen_parser.add_argument(
        "--shared-drive-url",
        required=True,
        help="Shared Drive root URL or ID that contains L0/L1 data and templates.",
    )
    gen_parser.add_argument(
        "--credentials",
        dest="credentials",
        default=None,
        help="Path to the service account JSON key file. Defaults to service-account-credentials.json in the project root.",
    )
    gen_parser.add_argument(
        "--entities",
        type=str,
        help='Comma-separated list of entity names to process (e.g., "Hyundai,Volvo"). If not provided, all entities from the Google Sheet will be processed.',
    )
    gen_parser.set_defaults(func=_run_generate_data)

    report_parser = subparsers.add_parser(
        "generate-report",
        help="Generate Google Slides reports from L1 data.",
    )
    report_parser.add_argument(
        "--shared-drive-file",
        required=True,
        help="Shared Drive root URL or ID that contains L1/L2 data, templates, and entities.",
    )
    report_parser.add_argument(
        "--credentials",
        dest="credentials",
        default=None,
        help="Path to the service account JSON key file. Defaults to service-account-credentials.json in the project root.",
    )
    report_parser.add_argument(
        "--entities",
        type=str,
        help='Comma-separated list of entity names to process (e.g., "Hyundai,Volvo"). If not provided, all entities from the entities file will be processed.',
    )
    report_parser.set_defaults(func=_run_generate_report)

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
