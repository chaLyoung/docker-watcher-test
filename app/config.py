"""
Docker Watcher 설정
"""
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional
from urllib.parse import quote

import yaml


@dataclass
class PostgresConfig:
    """PostgreSQL 설정"""
    host: str = "localhost"
    port: int = 5432
    database: str = "aetemdb"
    username: str = "postgres"
    password: str = "postgres"


@dataclass
class RabbitMQConfig:
    """RabbitMQ 설정"""
    enabled: bool = False
    host: str = "localhost"
    port: int = 5672
    username: str = "guest"
    password: str = "guest"

    @property
    def url(self) -> str:
        # 특수문자가 포함된 비밀번호를 URL 인코딩
        encoded_password = quote(self.password, safe='')
        return f"amqp://{self.username}:{encoded_password}@{self.host}:{self.port}/"


@dataclass
class WebhookConfig:
    """Webhook 설정"""
    token: str = ""
    delay_seconds: float = 5.0
    timeout: int = 30
    retry_count: int = 3


@dataclass
class DockerConfig:
    """Docker 감시 설정"""
    url: Optional[str] = None
    watch_images: set = None
    include_actions: set = None

    def __post_init__(self):
        if self.watch_images is None:
            self.watch_images = set()
        if self.include_actions is None:
            self.include_actions = {"start", "stop", "die"}


@dataclass
class QueueConfig:
    """개별 큐 설정"""
    name: str                           # 큐 이름 (e.g. A0001)
    analysis_type: str                  # 분석 유형 (DB 기록용)
    mode: str = "container"             # "container" | "service"
    image: str = ""                     # container 모드: Docker 이미지
    network: str = ""                   # container 모드: Docker 네트워크
    volumes: List[str] = field(default_factory=list)  # container 모드: 볼륨 마운트
    service_url: str = ""               # service 모드: 요청 URL


@dataclass
class Config:
    """애플리케이션 설정"""
    docker: DockerConfig
    rabbitmq: RabbitMQConfig
    webhook: WebhookConfig
    postgres: PostgresConfig
    queues: List[QueueConfig]
    log_dir: str = "/app/logs"
    data_root_path: str = "/deploy/data/aetem/app"

    @classmethod
    def load(cls) -> "Config":
        """환경변수와 YAML에서 설정 로드"""
        config_path = os.getenv("CONFIG_PATH", "/config/watcher.yml")
        yaml_data = _load_yaml(config_path)

        # Docker 설정
        docker = DockerConfig(
            url=os.getenv("DOCKER_BASE_URL"),
            watch_images=yaml_data.get("watch_images", set()),
            include_actions=yaml_data.get("include_actions", {"start", "stop", "die"}),
        )

        # RabbitMQ 설정
        rabbitmq = RabbitMQConfig(
            enabled=os.getenv("RABBITMQ_ENABLED", "").lower() in ("true", "1", "yes"),
            host=os.getenv("RABBITMQ_HOST", "localhost"),
            port=int(os.getenv("RABBITMQ_PORT", "5672")),
            username=os.getenv("RABBITMQ_USERNAME", "guest"),
            password=os.getenv("RABBITMQ_PASSWORD", "guest"),
        )

        # Webhook 설정
        webhook = WebhookConfig(
            token=os.getenv("WEBHOOK_TOKEN", ""),
            delay_seconds=float(os.getenv("WEBHOOK_DELAY_SECONDS", "5.0")),
            timeout=int(os.getenv("WEBHOOK_TIMEOUT", "30")),
            retry_count=int(os.getenv("WEBHOOK_RETRY_COUNT", "3")),
        )

        # PostgreSQL 설정
        postgres = PostgresConfig(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=int(os.getenv("POSTGRES_PORT", "5432")),
            database=os.getenv("POSTGRES_DATABASE", "aetemdb"),
            username=os.getenv("POSTGRES_USERNAME", "postgres"),
            password=os.getenv("POSTGRES_PASSWORD", "postgres"),
        )

        # 큐 설정 (YAML)
        queues = _parse_queues(yaml_data.get("queues", []))

        return cls(
            docker=docker,
            rabbitmq=rabbitmq,
            webhook=webhook,
            postgres=postgres,
            queues=queues,
            log_dir=os.getenv("LOG_DIR", "/app/logs"),
            data_root_path=os.getenv("DATA_ROOT_PATH", "/deploy/data/aetem/app"),
        )


def _parse_queues(raw: list) -> List[QueueConfig]:
    """YAML의 queues 섹션을 QueueConfig 리스트로 변환"""
    queues = []
    for item in raw:
        queues.append(QueueConfig(
            name=item["name"],
            analysis_type=item.get("analysis_type", ""),
            mode=item.get("mode", "container"),
            image=item.get("image", ""),
            network=item.get("network", ""),
            volumes=item.get("volumes", []),
            service_url=item.get("service_url", ""),
        ))
    return queues


def _load_yaml(path: str) -> dict:
    """YAML 파일 로드"""
    p = Path(path)
    if not p.exists():
        return {}

    data = yaml.safe_load(p.read_text(encoding="utf-8")) or {}
    result = {}

    # Docker watch 설정
    if watch := data.get("watch", {}):
        if images := watch.get("images"):
            result["watch_images"] = set(images)

    if events := data.get("events", {}):
        if actions := events.get("include_actions"):
            result["include_actions"] = {a.lower() for a in actions}

    # 큐 설정 (신규)
    if queues := data.get("queues"):
        result["queues"] = queues

    return result
