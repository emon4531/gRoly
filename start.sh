# Pella runs this file with Python, so this is a Python launcher (not bash).
import os, sys, subprocess, time

os.chdir(os.path.dirname(os.path.abspath(__file__)))
PY = sys.executable

print("Installing requirements...", flush=True)
cmd = [PY, "-m", "pip", "install", "-r", "requirements.txt", "--no-cache-dir", "-q"]
if subprocess.call(cmd) != 0:
    print("Retrying with --break-system-packages...", flush=True)
    if subprocess.call(cmd + ["--break-system-packages"]) != 0:
        print("WARNING: some packages failed to install", flush=True)

print("Starting bot (auto restart every 30 minutes)...", flush=True)
while True:
    try:
        subprocess.run([PY, "main.py"], timeout=1800)
    except subprocess.TimeoutExpired:
        pass
    print("Restarting bot in 5 seconds...", flush=True)
    time.sleep(5)
