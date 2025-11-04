"""CLI orchestrator for multi-source ingestion."""

from __future__ import annotations

import argparse
import importlib
import inspect
import logging
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import yaml

from src.data.config import PROJECT_ROOT


LOGGER = logging.getLogger(__name__)

DEFAULT_CONFIG_PATH = PROJECT_ROOT / "config" / "sources.yml"


def load_config(config_path: Path) -> Dict[str, List[Dict[str, Any]]]:
    with config_path.open("r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh) or {}
    return {str(league): entries for league, entries in data.items() if isinstance(entries, list)}


def _parse_seasons(args: argparse.Namespace) -> List[int]:
    seasons: List[int] = []
    if args.seasons:
        seasons.extend(int(season) for season in args.seasons)
    if args.season_start and args.season_end:
        seasons.extend(range(int(args.season_start), int(args.season_end) + 1))
    return sorted({int(season) for season in seasons})


def _invoke_handler(handler_path: str, *, kwargs: Dict[str, Any]) -> Any:
    module_name, function_name = handler_path.split(":", maxsplit=1)
    module = importlib.import_module(module_name)
    function = getattr(module, function_name)
    return function(**kwargs)


def _prepare_kwargs(
    handler_path: str,
    *,
    base_kwargs: Dict[str, Any],
    seasons: List[int],
    timeout: Optional[int],
    date: Optional[str] = None,
) -> Dict[str, Any]:
    module_name, function_name = handler_path.split(":", maxsplit=1)
    module = importlib.import_module(module_name)
    function = getattr(module, function_name)
    signature = inspect.signature(function)
    kwargs = dict(base_kwargs)

    if seasons and "seasons" in signature.parameters:
        kwargs.setdefault("seasons", seasons)
    if timeout and "timeout" in signature.parameters and "timeout" not in kwargs:
        kwargs["timeout"] = timeout
    if date and "date" in signature.parameters:
        kwargs["date"] = date

    return kwargs


def list_sources(config: Dict[str, List[Dict[str, Any]]]) -> None:
    for league, entries in config.items():
        LOGGER.info("%s:", league.upper())
        for entry in entries:
            LOGGER.info("  - %s (%s) -> %s", entry.get("key"), entry.get("category"), entry.get("handler"))


def run_sources(
    config: Dict[str, List[Dict[str, Any]]],
    *,
    leagues: Iterable[str] | None,
    sources: Iterable[str] | None,
    seasons: List[int],
    timeout: Optional[int],
    dry_run: bool,
) -> None:
    league_filter = {league.lower() for league in leagues} if leagues else None
    source_filter = {source for source in sources} if sources else None

    for league, entries in config.items():
        if league_filter and league.lower() not in league_filter:
            continue
        for entry in entries:
            key = entry.get("key")
            if not entry.get("enabled", True):
                LOGGER.info("Skipping disabled source %s", key)
                continue
            if source_filter and key not in source_filter:
                continue

            handler_path = entry.get("handler")
            if not handler_path:
                LOGGER.warning("Source %s missing handler path", key)
                continue

            params = entry.get("params") or {}
            # Support date parameter for historical odds sources
            date = params.pop("date", None)
            kwargs = _prepare_kwargs(handler_path, base_kwargs=params, seasons=seasons, timeout=timeout, date=date)
            LOGGER.info("Executing %s -> %s with args %s", key, handler_path, kwargs)
            if dry_run:
                continue
            try:
                result = _invoke_handler(handler_path, kwargs=kwargs)
                if result is not None:
                    LOGGER.info("%s completed. Result: %s", key, result)
            except Exception as exc:  # noqa: BLE001
                LOGGER.exception("Source %s failed: %s", key, exc)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run configured data source ingestions")
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG_PATH, help="Path to sources.yml")
    parser.add_argument("--league", action="append", help="Limit to one or more leagues (e.g. NFL, NBA)")
    parser.add_argument("--source", action="append", help="Limit to specific source keys (repeatable)")
    parser.add_argument("--seasons", nargs="*", type=int, help="Explicit list of seasons to request")
    parser.add_argument("--season-start", type=int, help="Start year for inclusive season range")
    parser.add_argument("--season-end", type=int, help="End year for inclusive season range")
    parser.add_argument("--timeout", type=int, help="Override request timeout in seconds")
    parser.add_argument("--dry-run", action="store_true", help="Print actions without executing handlers")
    parser.add_argument("--list", action="store_true", help="List configured sources and exit")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"])
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level))

    config_path: Path = args.config
    if not config_path.exists():
        raise SystemExit(f"Config file not found: {config_path}")

    config = load_config(config_path)
    if args.list:
        list_sources(config)
        return

    seasons = _parse_seasons(args)
    run_sources(
        config,
        leagues=args.league,
        sources=args.source,
        seasons=seasons,
        timeout=args.timeout,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()

