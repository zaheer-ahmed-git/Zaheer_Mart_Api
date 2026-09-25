from sqlmodel import Session, SQLModel, create_engine

from app import settings

connection_string = str(settings.DATABASE_URL).replace("postgresql", "postgresql+psycopg")

engine = create_engine(connection_string, pool_recycle=300, pool_size=10)


def create_table():
    SQLModel.metadata.create_all(engine)


def get_session():
    with Session(engine) as session:
        yield session
