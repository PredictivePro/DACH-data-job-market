from pathlib import Path


def load_env(path=".env"):
    """Load simple KEY=VALUE settings without overwriting managed environment values."""
    env_file = Path(path)
    if not env_file.exists():
        return
    import os
    for line in env_file.read_text(encoding="utf-8").splitlines():
        value = line.strip()
        if not value or value.startswith("#") or "=" not in value:
            continue
        key, item = value.split("=", 1)
        key, item = key.strip(), item.strip()
        if item[:1] == item[-1:] and item.startswith(("'", '"')):
            item = item[1:-1]
        if key and key.replace("_", "").isalnum():
            os.environ.setdefault(key, item)

