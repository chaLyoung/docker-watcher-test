"""
Docker Watcher + Multi-Queue Analysis Consumer

- Docker 컨테이너 이벤트 실시간 감시
- 여러 RabbitMQ 큐에서 개별 consume
  - container 모드: Docker 컨테이너 실행 → 종료 대기 → webhook
  - service 모드: HTTP 요청 → 응답 대기 → webhook
- 각 consumer는 분석 완료까지 다음 메시지를 consume하지 않음
"""
import asyncio
import json
import logging
import os
import signal
import sys
import uuid
import string
import shortuuid

from dataclasses import dataclass, field
from datetime import datetime
from logging.handlers import RotatingFileHandler
from typing import Optional

import aio_pika
import aiodocker
import httpx

from app.config import Config, QueueConfig
from app.database import Database

logger = logging.getLogger("watcher")


# ============================================================
# 통계
# ============================================================
@dataclass
class Stats:
    """처리 통계"""
    started_at: datetime = field(default_factory=datetime.now)
    # Docker Watcher
    docker_events_received: int = 0
    docker_events_matched: int = 0
    docker_events_filtered: int = 0
    # Analysis Consumer
    mq_messages_received: int = 0
    analysis_success: int = 0
    analysis_failed: int = 0


# ============================================================
# Webhook 전송
# ============================================================
class WebhookSender:
    """Webhook 전송기"""

    def __init__(self, token: str, timeout: int = 30, retry_count: int = 3):
        self.token = token
        self.retry_count = retry_count
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(timeout),
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {token}",
            },
        )

    async def send(self, url: str, payload: dict) -> bool:
        """Webhook 전송 (재시도 포함)"""
        data = json.dumps(payload, ensure_ascii=False)

        for attempt in range(self.retry_count):
            try:
                response = await self._client.post(url, content=data)

                if response.status_code < 300:
                    logger.info("Webhook success: url=%s status=%d", url, response.status_code)
                    return True

                logger.warning(
                    "Webhook error: url=%s status=%d (attempt %d/%d)",
                    url, response.status_code, attempt + 1, self.retry_count,
                )

            except httpx.RequestError as e:
                logger.warning(
                    "Webhook failed: url=%s error=%s (attempt %d/%d)",
                    url, e, attempt + 1, self.retry_count,
                )

            if attempt < self.retry_count - 1:
                await asyncio.sleep(1.0 * (attempt + 1))

        logger.error("Webhook failed after %d attempts: url=%s", self.retry_count, url)
        return False

    async def close(self):
        await self._client.aclose()


# ============================================================
# Analysis Consumer (큐 1개 = 인스턴스 1개)
# ============================================================
class AnalysisConsumer:
    """
    개별 큐 Consumer

    - container 모드: Docker 컨테이너 실행 → 종료 대기
    - service 모드: HTTP 요청 → 응답 대기
    - 분석 완료까지 다음 메시지를 consume하지 않음
    """

    def __init__(
        self,
        queue_config: QueueConfig,
        config: Config,
        stats: Stats,
        webhook: WebhookSender,
        db: Database,
        docker: aiodocker.Docker,
    ):
        self.qcfg = queue_config
        self.config = config
        self.stats = stats
        self.webhook = webhook
        self.db = db
        self.docker = docker
        self._shutdown = asyncio.Event()
        self._http = httpx.AsyncClient(timeout=httpx.Timeout(300))

    async def start(self):
        """큐 연결 후 메시지 consume (재연결 포함)"""
        qname = self.qcfg.name
        backoff = 1.0

        while not self._shutdown.is_set():
            try:
                connection = await aio_pika.connect_robust(self.config.rabbitmq.url)
                logger.info("[%s] Connected to RabbitMQ", qname)
                backoff = 1.0

                channel = await connection.channel()
                await channel.set_qos(prefetch_count=1)
                queue = await channel.declare_queue(qname, durable=True)

                logger.info("[%s] Consuming (mode=%s)", qname, self.qcfg.mode)

                async with queue.iterator() as queue_iter:
                    async for message in queue_iter:
                        if self._shutdown.is_set():
                            break

                        async with message.process():
                            await self._handle_message(message)

            except asyncio.CancelledError:
                break
            except Exception as e:
                if self._shutdown.is_set():
                    break

                logger.error("[%s] RabbitMQ error: %s (retry in %.1fs)", qname, e, backoff)
                
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30.0)

    async def _handle_message(self, message: aio_pika.IncomingMessage):
        """메시지 처리: 파일 확인 → DB INSERT → 분석 실행 → DB UPDATE → Webhook"""
        qname = self.qcfg.name
        self.stats.mq_messages_received += 1
        seq = None

        try:
            headers = message.properties.headers or {}
            logger.info("headers=%s", str(headers))
            req_id = headers.get("x-aetem-log-trace-id")

            body = json.loads(message.body.decode())

            analysis_type = body.get("type", "")
            situation_id = body.get("situationId", "")
            brigade_phase_id = body.get("brigadePhaseId", "")
            battalion_phase_id = body.get("battalion_phase_id", "")
            callback_url = body.get("callbackUrl", "")
            opord_path = body.get("opordPath", "").replace("\\", "/")
            req_id = body.get("requestId", "")
            requestDateTime  = body.get("requestDateTime", "")
            publish_time = datetime.fromtimestamp(int(requestDateTime) / 1000) if requestDateTime else None

            if not req_id:
                alphabet = string.ascii_lowercase + string.digits
                su = shortuuid.ShortUUID(alphabet=alphabet)
                req_id = str(su.random(length=8))
                logger.warning("[%s] No x-aetem-log-trace-id in header, generated: %s", qname, req_id)

            logger.info(
                "[%s] Message received: type=%s situationId=%s brigadePhaseId=%s battalion_phase_id=%s req_id=%s publish_time=%s",
                qname, analysis_type, situation_id, brigade_phase_id, battalion_phase_id, req_id, publish_time,
            )

            if not callback_url:
                logger.warning("[%s] No callbackUrl in message, skipping", qname)
                return

            # 파일 경로 확인
            full_path = os.path.join(self.config.data_root_path, opord_path)
            output_path = os.path.dirname(full_path).replace("/input", "/output")
            save_path = os.path.join(output_path, req_id)
            os.makedirs(save_path, exist_ok=True)

            if not os.path.exists(full_path):
                logger.warning("[%s] File not found: %s, skipping", qname, full_path)
                return

            logger.info("[%s] File exists: %s", qname, full_path)

            # 1) DB INSERT
            try:
                seq = await self.db.insert_analysis_history(
                    analysis_type=analysis_type,
                    req_id=req_id,
                    publish_time=publish_time,
                    status="P",
                )
                logger.info("[%s] DB insert success: seq=%d", qname, seq)
            except Exception as e:
                logger.error("[%s] DB insert failed: %s", qname, e)

            # 2) 분석 실행 (모드에 따라 분기)
            try:
                if self.qcfg.mode == "container":
                    success = await self._run_container(body)
                else:
                    success = await self._call_service(full_path, req_id, save_path)
            except Exception as e:
                logger.exception("[%s] Analysis error: %s", qname, e)
                success = False

            if success:
                self.stats.analysis_success += 1
            else:
                self.stats.analysis_failed += 1

            # 3) DB UPDATE
            if seq is not None:
                status = "S" if success else "F"
                try:
                    await self.db.update_analysis_history(seq=seq, status=status)
                    logger.info("[%s] DB update result: seq=%d status=%s", qname, seq, status)
                except Exception as e:
                    logger.error("[%s] DB update failed: %s", qname, e)

            # 4) Webhook 전송
            # # 5초 대기
            # delay = self.config.webhook.delay_seconds
            # logger.info("Waiting %.1f seconds before webhook...", delay)
            # await asyncio.sleep(delay)
            payload = {
                'situationId': situation_id,
                'brigadePhaseId': brigade_phase_id,
                'reqId': req_id,
                'preproccessingPath': os.path.join(output_path, 'json', 'result.json'),
            }


            success = await self.webhook.send(callback_url, payload)
            if success:
                logger.info("[%s] webhook send success: seq=%d", qname, seq)
            else:
                logger.info("[%s] webhook send failed: seq=%d", qname, seq)

        except json.JSONDecodeError as e:
            logger.error("[%s] Invalid JSON message: %s", qname, e)
        except Exception as e:
            logger.exception("[%s] Message handling error: %s", qname, e)

    # ── Container 모드 ──────────────────────────────────────

    async def _run_container(self, body: dict) -> bool:
        """Docker 컨테이너 실행 후 종료 대기"""
        qname = self.qcfg.name
        container_name = f"{qname}-{uuid.uuid4().hex[:8]}"

        env_vars = self._build_env_vars(body)

        container_config = {
            "Image": self.qcfg.image,
            "Env": [f"{k}={v}" for k, v in env_vars.items()],
            "HostConfig": {
                "Binds": self.qcfg.volumes,
                "NetworkMode": self.qcfg.network or "bridge",
                "AutoRemove": False,
            },
        }

        container = None
        try:
            logger.info("[%s] Creating container: image=%s name=%s", qname, self.qcfg.image, container_name)
            container = await self.docker.containers.create_or_run(
                config=container_config, name=container_name,
            )
            await container.start()
            logger.info("[%s] Container started: %s", qname, container_name)

            # 컨테이너 종료 대기
            result = await container.wait()
            exit_code = result.get("StatusCode", -1)
            logger.info("[%s] Container finished: name=%s exit_code=%d", qname, container_name, exit_code)

            return exit_code == 0

        except Exception as e:
            logger.error("[%s] Container error: name=%s error=%s", qname, container_name, e)
            return False

        finally:
            if container:
                try:
                    await container.delete(force=True)
                    logger.info("[%s] Container removed: %s", qname, container_name)
                except Exception:
                    pass

    def _build_env_vars(self, body: dict) -> dict:
        """메시지 본문에서 컨테이너 환경변수 생성"""
        env = {}
        for key in ("situationId", "brigadePhaseId", "opordPath", "callbackUrl"):
            value = body.get(key)
            if value is not None:
                env[key.upper()] = str(value)

        env["DATA_ROOT_PATH"] = self.config.data_root_path
        env["ANALYSIS_TYPE"] = self.qcfg.analysis_type
        return env

    # ── Service 모드 ────────────────────────────────────────

    async def _call_service(self, full_path: str, req_id: str, save_path: str) -> bool:
        """이미 실행 중인 Docker 서비스에 HTTP 요청"""
        qname = self.qcfg.name
        url = self.qcfg.service_url

        try:
            logger.info("[%s] Calling service: %s", qname, url)
            
            payload = {
                "req_id": req_id,
                "input_path": full_path,
                "output_path": save_path,
            }
            start_time = datetime.now()
            response = await self._http.post(url, json=payload)
            end_time = datetime.now()
            elapsed = (end_time - start_time).total_seconds()

            logger.info("[%s] Service finished: elapsed=%.2fs (start=%s end=%s)",
                        qname, elapsed,
                        start_time.strftime("%H:%M:%S.%f")[:-3],
                        end_time.strftime("%H:%M:%S.%f")[:-3])

            if response.status_code < 300:
                logger.info("[%s] Service success: status=%d", qname, response.status_code)
                return True

            logger.warning("[%s] Service error: status=%d body=%s", qname, response.status_code, response.text[:200])
            return False

        except Exception as e:
            logger.error("[%s] Service call failed: %s", qname, e)
            return False

    # ── Lifecycle ───────────────────────────────────────────

    def shutdown(self):
        self._shutdown.set()

    async def close(self):
        await self._http.aclose()


# ============================================================
# Docker Watcher
# ============================================================
class DockerWatcher:
    """Docker 이벤트 감시기"""

    def __init__(self, config: Config, stats: Stats):
        self.config = config
        self.stats = stats
        self._shutdown = asyncio.Event()
        self._docker: Optional[aiodocker.Docker] = None

    async def start(self):
        """감시 시작"""
        backoff = 1.0

        while not self._shutdown.is_set():
            try:
                self._docker = aiodocker.Docker(url=self.config.docker.url)
                logger.info("Connected to Docker daemon")
                backoff = 1.0

                subscriber = self._docker.events.subscribe(filters={"type": ["container"]})

                while not self._shutdown.is_set():
                    event = await subscriber.get()
                    if event is None:
                        continue

                    self.stats.docker_events_received += 1

                    action = (event.get("Action") or "").lower()
                    if action not in self.config.docker.include_actions:
                        self.stats.docker_events_filtered += 1
                        continue

                    image = self._extract_image(event)
                    if self.config.docker.watch_images and image not in self.config.docker.watch_images:
                        self.stats.docker_events_filtered += 1
                        continue

                    self.stats.docker_events_matched += 1
                    actor = event.get("Actor") or {}
                    attrs = actor.get("Attributes") or {}

                    logger.info(
                        "Docker Event: action=%s image=%s name=%s id=%.12s",
                        action, image, attrs.get("name"), actor.get("ID", ""),
                    )

            except asyncio.CancelledError:
                break
            except Exception as e:
                if self._shutdown.is_set():
                    break

                logger.error("Docker error: %s (retry in %.1fs)", e, backoff)

                if self._docker:
                    await self._docker.close()
                    self._docker = None

                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30.0)

    @staticmethod
    def _extract_image(event: dict) -> Optional[str]:
        actor = event.get("Actor") or {}
        attrs = actor.get("Attributes") or {}
        image = attrs.get("image")
        if not image:
            return None
        return image.split("@", 1)[0].strip()

    def shutdown(self):
        self._shutdown.set()

    async def close(self):
        if self._docker:
            await self._docker.close()


# ============================================================
# 메인 애플리케이션
# ============================================================
class Application:
    """메인 애플리케이션"""

    def __init__(self, config: Config):
        self.config = config
        self.stats = Stats()
        self._shutdown = asyncio.Event()

        self.db = Database(config.postgres)
        self.webhook = WebhookSender(
            token=config.webhook.token,
            timeout=config.webhook.timeout,
            retry_count=config.webhook.retry_count,
        )
        self.docker_client = aiodocker.Docker(url=config.docker.url)
        self.docker_watcher = DockerWatcher(config, self.stats)
        self.consumers: list[AnalysisConsumer] = []

    async def start(self):
        """애플리케이션 시작"""
        tasks = []

        # DB 연결 (실패 시 앱 종료)
        try:
            await self.db.connect()
            logger.info("Database ready")
        except ConnectionError as e:
            logger.error("Application cannot start: %s", e)
            return

        # Docker Watcher
        if self.config.docker.watch_images:
            logger.info("Docker watching images: %s", ", ".join(sorted(self.config.docker.watch_images)))
            logger.info("Docker watching actions: %s", ", ".join(sorted(self.config.docker.include_actions)))
            tasks.append(asyncio.create_task(self.docker_watcher.start(), name="docker-watcher"))
        else:
            logger.info("Docker watcher disabled (no images configured)")

        # 큐별 Analysis Consumer
        if self.config.rabbitmq.enabled and self.config.queues:
            for qcfg in self.config.queues:
                consumer = AnalysisConsumer(
                    queue_config=qcfg,
                    config=self.config,
                    stats=self.stats,
                    webhook=self.webhook,
                    db=self.db,
                    docker=self.docker_client,
                )
                self.consumers.append(consumer)
                tasks.append(asyncio.create_task(consumer.start(), name=f"consumer-{qcfg.name}"))

            logger.info(
                "Consumers started: %d queues (container=%d, service=%d)",
                len(self.consumers),
                sum(1 for q in self.config.queues if q.mode == "container"),
                sum(1 for q in self.config.queues if q.mode == "service"),
            )
        else:
            logger.info("RabbitMQ consumers disabled")

        if not tasks:
            logger.warning("No tasks configured!")
            return

        logger.info("Application started")

        try:
            await self._shutdown.wait()
        finally:
            await self._cleanup(tasks)

    async def _cleanup(self, tasks):
        """정리"""
        logger.info("Shutting down...")

        self.docker_watcher.shutdown()
        for consumer in self.consumers:
            consumer.shutdown()

        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

        await self.docker_watcher.close()
        for consumer in self.consumers:
            await consumer.close()
        await self.webhook.close()
        await self.docker_client.close()
        await self.db.close()

        s = self.stats
        logger.info(
            "Stats: docker(recv=%d match=%d filter=%d) mq(recv=%d ok=%d fail=%d) db(ok=%d fail=%d)",
            s.docker_events_received, s.docker_events_matched, s.docker_events_filtered,
            s.mq_messages_received, s.analysis_success,  s.analysis_failed,
        )

    def shutdown(self):
        self._shutdown.set()


# ============================================================
# 로깅
# ============================================================
def setup_logging(log_dir: str):
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    logger.setLevel(logging.INFO)

    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(fmt)
    logger.addHandler(console)

    os.makedirs(log_dir, exist_ok=True)
    file_handler = RotatingFileHandler(
        os.path.join(log_dir, "watcher.log"),
        maxBytes=10 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
    )
    file_handler.setFormatter(fmt)
    logger.addHandler(file_handler)


# ============================================================
# 엔트리포인트
# ============================================================
async def main():
    config = Config.load()
    setup_logging(config.log_dir)

    app = Application(config)

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, app.shutdown)

    await app.start()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Interrupted")
