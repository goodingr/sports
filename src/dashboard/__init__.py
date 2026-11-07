"""Dashboard package for forward testing visualisation."""


def run(debug: bool = False, port: int = 8050, host: str = "0.0.0.0") -> None:
    """Load the dashboard application when its server is requested."""
    from .app import run as run_dashboard

    run_dashboard(debug=debug, port=port, host=host)


__all__ = ["run"]


