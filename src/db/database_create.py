"""This script creates a PostgreSQL database if it does not exist."""

import os
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv
from .db_conection import connect_db

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ENV_PATH = os.path.join(BASE_DIR, ".env")
load_dotenv(ENV_PATH)

db_name = os.getenv("DB_NAME")


def create_database():
    """Create a PostgreSQL database if it does not exist."""
    engine = None
    try:
        engine = connect_db("postgres")
        with engine.connect() as connection:
            connection.execution_options(isolation_level="AUTOCOMMIT")

            result = connection.execute(
                text("SELECT 1 FROM pg_database WHERE datname = :db_name;"),
                {"db_name": db_name},
            ).fetchone()
            if not result:
                connection.execute(text(f'CREATE DATABASE "{db_name}";'))
                print(f"Database '{db_name}' successfully created.")
            else:
                print(f"Database '{db_name}' already exists.")

    except SQLAlchemyError as e:
        print(f"Database error: {e}")
    except KeyError as e:
        print(f"Missing environment variable: {e}")
    except TypeError as e:
        print(f"Type error: {e}")
    except OSError as e:
        print(f"OS error: {e}")
    except RuntimeError as e:
        print(f"Unexpected runtime error: {e}")

    finally:
        if engine is not None:
            engine.dispose()
