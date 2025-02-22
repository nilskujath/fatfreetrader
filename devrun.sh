poetry env activate
poetry install
poetry run black .
python3 main.py