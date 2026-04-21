"""
Docker Watcher 설정
"""
import os
from dataclasses import dataclass, field, asdict
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
class WebhookPayload:
    """Webhook 전송 페이로드

    - service 모드: success, message, requestId, situationId, brigadePhaseId, preproccessingPath
    - container 모드 (tiff): success, message, requestId, gisTiffPath, cogTiffPath, resultPath
    - container 모드 (shape): success, message, requestId, shapePath, resultPath
    """
    success: bool = True
    message: Optional[str] = None
    failCode: Optional[str] = None
    requestId: Optional[str] = None
    # service 모드용
    situationId: Optional[str] = None
    brigadePhaseId: Optional[str] = None
    preproccessingPath: Optional[str] = None
    # container 모드용
    gisTiffPath: Optional[str] = None
    cogTiffPath: Optional[str] = None
    shapePath: Optional[str] = None
    resultPath: Optional[str] = None
    requestUserId: Optional[str] = None
    battalionPhaseId: Optional[str] = None
    battalionAspectId: Optional[str] = None
    geospatialAnalysisType: Optional[str] = None
    eventPropagation: Optional[str] = None
    eventTypeName: Optional[str] = None
    analysisPipeline: Optional[str] = None

    def to_dict(self) -> dict:
        """None 필드 제외하고 dict 변환"""
        return {k: v for k, v in asdict(self).items() if v is not None}


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
    name: str                                           # 큐 이름 (e.g. A0001)
    analysis_type: str                                  # 분석 유형 (DB 기록용)
    mode: str = "container"                             # "container" | "service"
    image: str = ""                                     # container 모드: Docker 이미지
    network: str = ""                                   # container 모드: Docker 네트워크
    volumes: List[str] = field(default_factory=list)    # container 모드: 볼륨 마운트
    service_url: str = ""                               # service 모드: 요청 URL
    env_mapping: dict = field(default_factory=dict)
    response_type: str = "tiff"                         # 응답 타입: "tiff" | "shape"
    result_filename_map: dict = field(default_factory=dict)
    result_filename: str = ""                           # 산출물 파일명 (e.g. "viewshed.tif")
    env: dict = field(default_factory=dict)             # 고정 환경변수
    timeout: int = 600
    concurrency: int = 1


@dataclass
class Config:
    """애플리케이션 설정"""
    docker: DockerConfig
    rabbitmq: RabbitMQConfig
    webhook: WebhookConfig
    postgres: PostgresConfig
    queues: List[QueueConfig] = field(default_factory=list)
    log_dir: str = "/app/logs"
    data_root_path: str = "/deploy/data/aetem/app"
    spatial_data_path: str = "/deploy/data/aetem/app/gis"
    spatial_mount_path: str = "/deploy/data/aetem/app/gis"

    @classmethod
    def load(cls) -> "Config":
        """환경변수와 YAML에서 설정 로드"""
        config_path = os.getenv("CONFIG_PATH", "/config/watcher.yml")
        yaml_data = _load_yaml(config_path)

        docker = DockerConfig(
            url=os.getenv("DOCKER_BASE_URL"),
            watch_images=yaml_data.get("watch_images", set()),
            include_actions=yaml_data.get("include_actions", {"start", "stop", "die"}),
        )

        rabbitmq = RabbitMQConfig(
            enabled=os.getenv("RABBITMQ_ENABLED", "").lower() in ("true", "1", "yes"),
            host=os.getenv("RABBITMQ_HOST", "localhost"),
            port=int(os.getenv("RABBITMQ_PORT", "5672")),
            username=os.getenv("RABBITMQ_USERNAME", "guest"),
            password=os.getenv("RABBITMQ_PASSWORD", "guest"),
        )

        webhook = WebhookConfig(
            token=os.getenv("WEBHOOK_TOKEN", ""),
            delay_seconds=float(os.getenv("WEBHOOK_DELAY_SECONDS", "5.0")),
            timeout=int(os.getenv("WEBHOOK_TIMEOUT", "10")),
            retry_count=int(os.getenv("WEBHOOK_RETRY_COUNT", "3")),
        )

        postgres = PostgresConfig(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=int(os.getenv("POSTGRES_PORT", "5432")),
            database=os.getenv("POSTGRES_DATABASE", "aetemdb"),
            username=os.getenv("POSTGRES_USERNAME", "postgres"),
            password=os.getenv("POSTGRES_PASSWORD", "postgres"),
        )

        queues = _parse_queues(yaml_data.get("queues", []))

        return cls(
            docker=docker,
            rabbitmq=rabbitmq,
            webhook=webhook,
            postgres=postgres,
            queues=queues,
            log_dir=os.getenv("LOG_DIR", "/app/logs"),
            data_root_path=os.getenv("DATA_ROOT_PATH", "/deploy/data/aetem/app"),
            spatial_data_path=os.getenv("SPATIAL_DATA_PATH", "/deploy/data/aetem/app/gis"),
            spatial_mount_path=os.getenv("SPATIAL_MOUNT_PATH", "/deploy/data/aetem/app/gis"),
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
            env_mapping=item.get("env_mapping", {}),
            response_type=item.get("response_type", "tiff"),
            result_filename=item.get("result_filename", ""),
            result_filename_map=item.get("result_filename_map", {}),
            timeout=item.get("timeout", 600),
            env=item.get("env", {}),
            concurrency=item.get("concurrency", 1),
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
