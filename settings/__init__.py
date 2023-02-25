import os
import re
from pathlib import Path

from dynaconf import LazySettings

from settings.validators import validators


def get_settings_files():
    settings_dir = str(Path(__file__).resolve().parent)
    return [f for f in os.listdir(settings_dir) if re.match(".[a-zA-Z0-9]+.json", os.path.basename(f))]


settings = LazySettings(
    environments=True,
    envvar_prefix=False,
    settings_files=[os.path.join("settings", f) for f in get_settings_files()],
    env_switcher="ETL_ENV",
    load_dotenv=True,
    root_path=str(Path(__file__).resolve().parent.parent),
    validators=validators,
)
