from sqlmodel import Session, create_engine, select

from .ThriftMessage import ThriftMessage


def get_thrift_message(path: str, limit: int, method: str | None = None) -> list[ThriftMessage]:
    engine = create_engine(path)
    messages = []
    with Session(engine) as session:
        if method:
            statement = select(ThriftMessage).where(ThriftMessage.method == method).limit(limit)
        else:
            statement = select(ThriftMessage).limit(limit)
        results = session.exec(statement)
        for result in results:
            messages.append(result)
    return messages
