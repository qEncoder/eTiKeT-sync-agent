import importlib, pkgutil
from logging.config import fileConfig

from sqlalchemy import create_engine
from alembic import context

from etiket_sync_agent.models.base import Base
from etiket_sync_agent.db import DATABASE_URL

def import_all_models(package):
    package = importlib.import_module(package)
    for _, module_name, _ in pkgutil.iter_modules(package.__path__):
        print(f"{package.__name__}.{module_name}")
        importlib.import_module(f"{package.__name__}.{module_name}")

import_all_models('etiket_sync_agent.models')


config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)


target_metadata = Base.metadata

def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.

    """
    context.configure(
        url=DATABASE_URL,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.

    """
    connectable = create_engine(DATABASE_URL)

    with connectable.connect() as connection:
        context.configure(
            connection=connection, target_metadata=target_metadata
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
