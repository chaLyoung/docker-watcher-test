FROM python:3.11-slim

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl tzdata libexpat1 \
    && ln -sf /usr/share/zoneinfo/Asia/Seoul /etc/localtime \
    && rm -rf /var/lib/apt/lists/*

ADD https://astral.sh/uv/install.sh /tmp/uv-install.sh
RUN sh /tmp/uv-install.sh && rm -f /tmp/uv-install.sh
ENV PATH="/root/.local/bin:${PATH}"

COPY requirements.txt ./
RUN uv pip install --system -r requirements.txt

COPY ./app ./app

ENV PYTHONUNBUFFERED=1

CMD ["python", "-m", "app.main"]