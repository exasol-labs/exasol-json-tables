from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
PYTHON = ROOT / "python"

python_path = str(PYTHON)
if python_path in sys.path:
    sys.path.remove(python_path)

sys.path.insert(0, python_path)
