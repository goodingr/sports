
import os
import glob

print("Listing model files in models/:")
for f in glob.glob("models/*.pkl"):
    print(f"{f}: {os.path.getsize(f)} bytes")
