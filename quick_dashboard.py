# Quick launch for NEW Dash Dashboard
import subprocess
import sys

print("=" * 60)
print("🚀 ElliptiGraph - Quick Dash Dashboard")
print("=" * 60)
print("✨ Modern dashboard with better performance")
print("📍 Will open at: http://localhost:8050")
print("🔄 Press Ctrl+C to stop\n")

try:
    subprocess.run([
        sys.executable,
        "visualization/dash_app.py"
    ])
except KeyboardInterrupt:
    print("\n👋 Stopped")
