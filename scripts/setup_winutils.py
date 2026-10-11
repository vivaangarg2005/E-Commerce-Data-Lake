"""Utility to download winutils.exe and hadoop.dll for Windows local Spark execution."""
import os
import urllib.request
from pathlib import Path


def setup_hadoop_home():
    base_dir = Path(__file__).resolve().parent.parent / ".hadoop"
    bin_dir = base_dir / "bin"
    bin_dir.mkdir(parents=True, exist_ok=True)

    winutils_exe = bin_dir / "winutils.exe"
    hadoop_dll = bin_dir / "hadoop.dll"

    urls = {
        winutils_exe: "https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.3.6/bin/winutils.exe",
        hadoop_dll: "https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.3.6/bin/hadoop.dll",
    }

    for path, url in urls.items():
        if not path.exists() or path.stat().st_size == 0:
            print(f"Downloading {path.name} from {url}...")
            try:
                urllib.request.urlretrieve(url, path)
                print(f"Downloaded {path.name} ({path.stat().st_size} bytes)")
            except Exception as e:
                print(f"Warning: could not download {path.name}: {e}")

    if winutils_exe.exists():
        os.environ["HADOOP_HOME"] = str(base_dir)
        os.environ["PATH"] = f"{bin_dir};{os.environ.get('PATH', '')}"
        print(f"HADOOP_HOME set to {base_dir}")
        return str(base_dir)
    return None

if __name__ == "__main__":
    setup_hadoop_home()
