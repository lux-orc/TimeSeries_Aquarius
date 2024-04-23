# This purges any files in the data folders receiving the CSV files and the out folder

import shutil
from pathlib import Path
from typing import Any


def cp(s: Any = '', /, display: int = 0, fg: int = 39, bg: int = 48) -> str:
    """Return the string for color print in the (IPython) console"""
    return f'\033[{display};{fg};{bg}m{s}\033[0m'


# Set up the main folder
path = Path.cwd()

# Clean the <data> folder
for p in (path_data := path / 'out').rglob('*'):
    # if p.parent == path_data:  # Skip any files in the <out> folder
    #     continue
    if p.is_dir():
        shutil.rmtree(p)
        print(cp(f'Folder <{p}> has been removed!', fg=31))
    if p.is_file():
        p.unlink()
        print(cp(f"File '{p}' has been removed!", fg=31))

# # Remove the <out> folder
# if (path_out := path / 'out').exists():
#     shutil.rmtree(path_out)
#     print(f'Folder <{path_out}> has been removed!')
