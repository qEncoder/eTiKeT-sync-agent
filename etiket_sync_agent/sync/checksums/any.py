import hashlib
from typing import Any

def md5(file_path, blocksize=65536) -> Any:
    m = hashlib.md5()
    
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(blocksize), b""):
            m.update(chunk)
    return m