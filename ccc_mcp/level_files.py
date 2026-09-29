"""File IDs required to complete a level pack."""

import re


def required_level_files(files):
    """Exclude example inputs, including named paths, from pack requirements."""
    return list(dict.fromkeys(
        str(item) for item in files
        if isinstance(item, (str, int))
        and "example" not in re.split(r"[^a-z0-9]+", str(item).lower())
    ))
