from sqlalchemy import JSON, TIMESTAMP, BigInteger, Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func

Base = declarative_base()


class Notification(Base):
    __tablename__ = 'notifications'

    id = Column(Integer, primary_key=True, autoincrement=True)
    event_id = Column(String, unique=True, nullable=False)
    event_type = Column(String, nullable=False)
    order_id = Column(BigInteger, nullable=True)
    user_id = Column(BigInteger, nullable=True)
    payload = Column(JSON, nullable=False)
    created_at = Column(TIMESTAMP, server_default=func.now(), nullable=False)

    def __repr__(self):
        return (
            f'<Notification(event_id={self.event_id}, event_type={self.event_type},'
            f'order_id={self.order_id}, user_id={self.user_id})>'
        )
