"""Run pylint and pytest with tqdm progress bars."""
import subprocess
import sys
from pathlib import Path

from tqdm import tqdm
import pytest

PY_FILES = ["core.py", "scan.py", "test.py", "volume_math.py"]


def run_pylint() -> None:
    """Run pylint on all target files with a progress bar."""
    for path in PY_FILES:
        if not Path(path).is_file():
            raise FileNotFoundError(f"Missing required file: {path}")

    with tqdm(total=len(PY_FILES), desc="pylint") as pbar:
        for path in PY_FILES:
            result = subprocess.run([
                "pylint",
                path,
            ], text=True, capture_output=True)
            sys.stdout.write(result.stdout)
            sys.stdout.flush()
            if result.returncode != 0:
                raise SystemExit(f"pylint failed for {path}")
            pbar.update(1)


def run_pytest() -> None:
    """Run pytest with a tqdm progress bar for tests."""

    class TqdmPlugin:  # pylint: disable=too-few-public-methods
        """pytest plugin showing progress with tqdm."""

        def __init__(self) -> None:
            self.pbar = None

        def pytest_collection_modifyitems(self, session, config, items):  # noqa: ARG002
            self.pbar = tqdm(total=len(items), desc="pytest")

        def pytest_runtest_logreport(self, report):
            if report.when == "call" and self.pbar:
                self.pbar.update(1)

        def pytest_sessionfinish(self, session, exitstatus):  # noqa: ARG002
            if self.pbar:
                self.pbar.close()

    errno = pytest.main(["test.py"], plugins=[TqdmPlugin()])
    if errno != 0:
        raise SystemExit(errno)


def main() -> None:
    """Execute pylint and pytest with progress bars."""
    run_pylint()
    run_pytest()


if __name__ == "__main__":
    main()
