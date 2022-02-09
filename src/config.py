from pathlib import Path
import datetime


PROJECT_DIR = Path(__file__).resolve().parent.parent

DEFAULT_DB_NAME = PROJECT_DIR / datetime.date.today().strftime("nasdaq-%Y-%m.sqlite3")

