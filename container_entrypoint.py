from __future__ import annotations

import os
import pwd
import sys


DATA_DIR = "/app/data"
APP_UID = 10001


def main() -> None:
    os.makedirs(DATA_DIR, exist_ok=True)
    account = pwd.getpwuid(APP_UID)

    # Bind mounts hide the image directory and its ownership. Make the mounted
    # application data writable before running the server as the unprivileged
    # application user.
    for current, directories, files in os.walk(DATA_DIR, followlinks=False):
        os.chown(current, APP_UID, account.pw_gid, follow_symlinks=False)
        for name in directories + files:
            os.chown(
                os.path.join(current, name),
                APP_UID,
                account.pw_gid,
                follow_symlinks=False,
            )

    os.setgid(account.pw_gid)
    os.setuid(APP_UID)
    os.execv(sys.executable, [sys.executable, "server.py"])


if __name__ == "__main__":
    main()
