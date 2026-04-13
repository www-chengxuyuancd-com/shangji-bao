FROM docker.1ms.run/python:3.12-slim

RUN sed -i 's|deb.debian.org|mirrors.aliyun.com|g' /etc/apt/sources.list.d/debian.sources && \
    apt-get update && \
    apt-get install -y --no-install-recommends curl libatomic1 g++ && \
    rm -rf /var/lib/apt/lists/*

RUN pip install -i https://pypi.tuna.tsinghua.edu.cn/simple uv

WORKDIR /app

COPY pyproject.toml README.md ./

RUN uv sync --no-dev --index-url https://pypi.tuna.tsinghua.edu.cn/simple

COPY prisma ./prisma
RUN uv run prisma generate

COPY scrapy.cfg ./
COPY src ./src
COPY scripts ./scripts

EXPOSE 5000

CMD ["uv", "run", "gunicorn", "-w", "4", "-b", "0.0.0.0:5000", "src.app:create_app()"]
