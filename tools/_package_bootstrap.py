from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
PYTHON = ROOT / "python"

if str(PYTHON) not in sys.path:
    sys.path.insert(0, str(PYTHON))
