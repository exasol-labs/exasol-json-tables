from pathlib import Path
import os
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

# Keep subprocess-based test helpers on the same interpreter as the parent
# test run, even when the host `python3` points at an older unsupported build.
python_bin = str(Path(sys.executable).resolve().parent)
path_entries = os.environ.get("PATH", "").split(os.pathsep) if os.environ.get("PATH") else []
if not path_entries or path_entries[0] != python_bin:
    os.environ["PATH"] = os.pathsep.join([python_bin, *path_entries] if path_entries else [python_bin])
