"""Demo Job for the Celery layer.

``echo`` is the trivial round-trip Job: submit it, poll it, get the same
string back. It touches no Link, so it is routed to the Compute queue (the
default queue) and exists so the broker -> worker -> result-backend
round-trip through Redis can be proven without hardware.
"""

from seestar_mcp.jobs.celery_app import app


@app.task(name="seestar_mcp.jobs.tasks.echo")
def echo(message: str) -> str:
    """Echo ``message`` back, proving a Job round-trips through Redis."""
    return message
