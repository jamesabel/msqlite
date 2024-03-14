from pathlib import Path


def get_temp_dir() -> Path:
    temp_dir = Path("temp")
    temp_dir.mkdir(exist_ok=True)
    return temp_dir
