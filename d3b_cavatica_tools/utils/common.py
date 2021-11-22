def strip_path(path):
    _, _, fname = path.rpartition("/")
    return fname


def get_key(path):
    if path.startswith("s3://"):
        _, _, key = path.replace("s3://", "").partition("/")
        return key
    else:
        return path
