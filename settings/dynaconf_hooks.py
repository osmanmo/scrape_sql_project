import logging
import os
from pathlib import Path
from sys import platform

logger = logging.getLogger()
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.INFO)

BASE_DIR = str(Path(__file__).resolve().parent.parent)


def post(settings):
    data = {"dynaconf_merge": True}
    environment = _get_environment(settings.get("ETL_ENV"))

    data.update(
        {
            "BASE_DIR": BASE_DIR,
            "SQLALCHEMY_DATABASE_URI": _get_sql_alchemy_conn_string(
                db_name=settings.get("DB_NAME"),
                db_user=settings.get("DB_USER"),
                db_password=settings.get("DB_PASSWORD", default=""),
                db_host=settings.get("DB_HOST"),
                db_port=settings.get("DB_PORT", default="3306"),
                db_flavor=settings.get("DB_FLAVOR"),
            ),
            "ENVIRONMENT": environment,

        }
    )
    return data


def _get_environment(environment):
    if environment:
        environment = environment.strip().upper()
    else:
        if "linux" not in platform.strip().lower():
            environment = "LOCAL"
        else:
            environment = "DEV"

    return environment


def _get_sql_alchemy_conn_string(db_user, db_password, db_host, db_flavor, db_name, db_port):
    db_host = _get_db_host_name(db_host)
    if db_flavor == "mysql":
        conn_str = "mysql+pymysql://%s:%s@%s:%s/%s" % (db_user, db_password, db_host, db_port, db_name)
    elif db_flavor == "mssql":
        conn_str = "mssql+pymssql://%s:%s@%s:%s/%s" % (db_user, db_password, db_host, db_port, db_name)
    else:
        conn_str = "postgresql+psycopg2://%s:%s@%s:%s/%s" % (db_user, db_password, db_host, db_port, db_name)

    return conn_str


def _is_running_in_docker():
    path = '/proc/self/cgroup'
    return (
            os.path.exists('/.dockerenv') or
            os.path.isfile(path) and any('docker' in line for line in open(path))
    )


def _get_db_host_name(db_host):
    if db_host in ["localhost", "host.docker.internal"]:
        if _is_running_in_docker():
            return "host.docker.internal"
        else:
            return "localhost"
    else:
        return db_host
