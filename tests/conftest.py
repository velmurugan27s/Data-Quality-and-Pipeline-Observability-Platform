import os
import sys
from pathlib import Path

os.environ.setdefault("GX_ANALYTICS_ENABLED", "false")
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
