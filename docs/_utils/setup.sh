#! /bin/bash

if pwd | egrep -q '\s'; then
	echo "Working directory name contains one or more spaces."
	exit 1
fi

which python3 || { echo "Failed to find python3. Try installing Python for your operative system: https://www.python.org/downloads/" && exit 1; }
# install pipx
which pipx || python3 -m pip install --user pipx
python3 -m pipx ensurepath

# install poetry
which poetry || pipx install poetry
poetry --version || { echo "Failed to find or install poetry. Try installing it manually: https://python-poetry.org/docs/#installation" && exit 1; }
poetry install
