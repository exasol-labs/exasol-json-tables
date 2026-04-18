from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
PYTHON = ROOT / "python"
TOOLS = ROOT / "tools"

# Keep the real package ahead of compatibility wrappers so tool entrypoints
# do not shadow `exasol_json_tables` during test imports.
for path in [str(PYTHON), str(TOOLS)]:
    if path in sys.path:
        sys.path.remove(path)

sys.path.insert(0, str(PYTHON))
sys.path.insert(1, str(TOOLS))
