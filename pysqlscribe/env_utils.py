def str2bool(v: str) -> bool | None:
    if v is None or isinstance(v, bool):
        return v
    if v.lower() in ("true", "1"):
        return True
    elif v.lower() in ("false", "0"):
        return False
    else:
        return False
