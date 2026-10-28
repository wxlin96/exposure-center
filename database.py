# -*- coding: utf-8 -*-
"""数据库引擎与会话。"""
from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base, sessionmaker

from config import SQLALCHEMY_DATABASE_URL, MYSQL_POOL_SIZE

engine = create_engine(
    SQLALCHEMY_DATABASE_URL,
    pool_size=MYSQL_POOL_SIZE,
    pool_pre_ping=True,
    pool_recycle=3600,
    echo=False,
)

SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)
Base = declarative_base()


def get_db():
    """FastAPI 依赖：请求级数据库会话。"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def init_db() -> None:
    """建表（幂等）。生产环境也可直接用 sql/schema.sql 手工建表。"""
    import models  # noqa: F401  确保模型注册到 Base.metadata
    Base.metadata.create_all(bind=engine)
