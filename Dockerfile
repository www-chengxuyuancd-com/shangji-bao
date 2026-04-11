FROM python:3.12-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends curl libatomic1 g++ && \
    rm -rf /var/lib/apt/lists/*

RUN pip install uv

WORKDIR /app

COPY pyproject.toml README.md ./

RUN uv sync --no-dev

COPY prisma ./prisma
RUN uv run prisma generate

COPY scrapy.cfg ./
COPY src ./src
COPY scripts ./scripts

EXPOSE 5000

CMD ["uv", "run", "gunicorn", "-w", "4", "-b", "0.0.0.0:5000", "src.app:create_app()"]
