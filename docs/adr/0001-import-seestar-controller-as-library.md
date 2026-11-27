# Import seestar-controller as a library, not a subprocess

Status: accepted

seestar-mcp depends on seestar-controller via a uv path dependency and calls
its modules (`link`, `astrometry`, `photometry`, `analysis`, `store`)
in-process. Shelling out to the `seestar-controller` CLI was rejected: the CLI
only has `extract-key`, `key-path`, and `gui` — there are no headless observing
subcommands to call. The package is a hatchling src-layout build made for
import, the GUI (PyGObject) is an optional extra so headless imports work, and
`link.mock_scope` is importable for tests without hardware.

## Consequences

- seestar-mcp pins the same Python (3.14) as seestar-controller.
- The path dependency means the two repos must live side by side (or the
  dependency switches to a git/path URL) for installs and Docker builds.
