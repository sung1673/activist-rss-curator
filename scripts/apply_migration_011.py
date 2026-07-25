import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from curator.mysql_migration import main  # noqa: E402


if __name__ == "__main__":
    raise SystemExit(main())
