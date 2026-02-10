"""
Docker Watcher 설정
"""
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
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
    queue_name: str = "preprocess"
    
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
class Config:
    """애플리케이션 설정"""
    docker: DockerConfig
    rabbitmq: RabbitMQConfig
    webhook: WebhookConfig
    postgres: PostgresConfig
    log_dir: str = "/app/logs"
    data_root_path: str = "/deploy/data/aetem/app"  # 추가
    
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
            queue_name=os.getenv("RABBITMQ_QUEUE_NAME", "preprocess"),
        )
        
        # Webhook 설정
        webhook = WebhookConfig(
            token=os.getenv("WEBHOOK_TOKEN", ""),
            delay_seconds=float(os.getenv("WEBHOOK_DELAY_SECONDS", "5.0")),
            timeout=int(os.getenv("WEBHOOK_TIMEOUT", "30")),
            retry_count=int(os.getenv("WEBHOOK_RETRY_COUNT", "3")),
        )

        postgres = PostgresConfig(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=int(os.getenv("POSTGRES_PORT", "5432")),
            database=os.getenv("POSTGRES_DATABASE", "aetemdb"),
            username=os.getenv("POSTGRES_USERNAME", "postgres"),
            password=os.getenv("POSTGRES_PASSWORD", "postgres"),
        )

        
        return cls(
            docker=docker,
            rabbitmq=rabbitmq,
            webhook=webhook,
            postgres=postgres,
            log_dir=os.getenv("LOG_DIR", "/app/logs"),
            data_root_path=os.getenv("DATA_ROOT_PATH", "/deploy/data/aetem/app"),
        )


def _load_yaml(path: str) -> dict:
    """YAML 파일 로드"""
    p = Path(path)
    if not p.exists():
        return {}
    
    data = yaml.safe_load(p.read_text(encoding="utf-8")) or {}
    result = {}
    
    if watch := data.get("watch", {}):
        if images := watch.get("images"):
            result["watch_images"] = set(images)
    
    if events := data.get("events", {}):
        if actions := events.get("include_actions"):
            result["include_actions"] = {a.lower() for a in actions}
    
    return result
