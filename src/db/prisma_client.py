from prisma import Prisma

_client: Prisma | None = None


def get_prisma() -> Prisma:
    global _client
    if _client is None:
        _client = Prisma()
        _client.connect()
    return _client


def close_prisma():
    global _client
    if _client is not None:
        _client.disconnect()
        _client = None
