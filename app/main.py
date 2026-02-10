"""
Docker Watcher + RabbitMQ Consumer

- Docker 컨테이너 이벤트 실시간 감시
- RabbitMQ 큐에서 메시지 consume 후 webhook 전송
"""
import asyncio
import json
import logging
import os
import signal
import sys
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from logging.handlers import RotatingFileHandler
from typing import Optional

import aio_pika
import aiodocker
import httpx

from app.config import Config
from app.database import Database

logger = logging.getLogger("watcher")


# ============================================================
# 통계
# ============================================================
@dataclass
class Stats:
    """처리 통계"""
    started_at: datetime = field(default_factory=datetime.now)
    # Docker
    docker_events_received: int = 0
    docker_events_matched: int = 0
    docker_events_filtered: int = 0
    # RabbitMQ
    mq_messages_received: int = 0
    mq_webhook_success: int = 0
    mq_webhook_failed: int = 0


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
# RabbitMQ Consumer
# ============================================================
class RabbitMQConsumer:
    """RabbitMQ 메시지 Consumer"""
    
    def __init__(self, config: Config, stats: Stats, webhook: WebhookSender, db: Database):
        self.config = config
        self.stats = stats
        self.webhook = webhook
        self.db = db
        self._connection: Optional[aio_pika.Connection] = None
        self._shutdown = asyncio.Event()
    
    async def start(self):
        """Consumer 시작"""
        backoff = 1.0
        
        while not self._shutdown.is_set():
            try:
                self._connection = await aio_pika.connect_robust(
                    self.config.rabbitmq.url
                )
                logger.info("Connected to RabbitMQ: %s", self.config.rabbitmq.host)
                backoff = 1.0
                
                channel = await self._connection.channel()
                await channel.set_qos(prefetch_count=1)
                
                queue = await channel.declare_queue(
                    self.config.rabbitmq.queue_name,
                    durable=True,
                )
                
                logger.info("Consuming queue: %s", self.config.rabbitmq.queue_name)
                
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
                
                logger.error("RabbitMQ error: %s (retry in %.1fs)", e, backoff)
                
                if self._connection:
                    await self._connection.close()
                    self._connection = None
                
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30.0)
    
    async def _handle_message(self, message: aio_pika.IncomingMessage):
        """메시지 처리"""
        self.stats.mq_messages_received += 1
        seq = None

        try:
            body = json.loads(message.body.decode())
            
            # 변수 초기화
            analysis_type = body.get("type", "")
            situation_id = body.get("situationId", "")
            brigade_phase_id = body.get("brigadePhaseId", "")
            callback_url = body.get("callbackUrl", "")
            opord_path = body.get("opordPath", "").replace("\\", "/")
            
            logger.info(
                "Message received: type=%s situationId=%s brigadePhaseId=%s",
                analysis_type, situation_id, brigade_phase_id,
            )
            
            if not callback_url:
                logger.warning("No callbackUrl in message, skipping")
                return
            
            # 파일 경로 확인
            full_path = os.path.join(self.config.data_root_path, opord_path)
            
            if not os.path.exists(full_path):
                logger.warning("File not found: %s, skipping", full_path)
                return
            
            logger.info("File exists: %s", full_path)

            # 1) DB INSERT
            try:
                seq = await self.db.insert_analysis_history(
                    analysis_type=analysis_type,
                    req_id=str(uuid.uuid4()),
                    status="P",
                )
                logger.info("DB insert success: seq=%d", seq)
            except Exception as e:
                logger.error("DB insert failed: %s", e)
            
            # 5초 대기
            delay = self.config.webhook.delay_seconds
            logger.info("Waiting %.1f seconds before webhook...", delay)
            await asyncio.sleep(delay)

            # 2) DB UPDATE
            if seq is not None:
                try:
                    await self.db.update_analysis_history(seq=seq, status="S")
                    logger.info("DB update success: seq=%d", seq)
                except Exception as e:
                    logger.error("DB update failed: %s", e)
            else:
                await self.db.update_analysis_history(seq=seq, status="F")
                    logger.info("DB update success: seq=%d", seq)
                except Exception as e:
                    logger.error("DB update failed: %s", e)

            # 3) Webhook 전송
            payload = {
                'situationId': situation_id,
                'brigadePhaseId': brigade_phase_id,
                'preproccessingPath': '/aitestfile/english.json',
                'sixWsPath': '/aitestfile/5w1h_output_sample(kanana).json',
            }
            
            success = await self.webhook.send(callback_url, payload)
            
            if success:
                self.stats.mq_webhook_success += 1
            else:
                self.stats.mq_webhook_failed += 1
                
        except json.JSONDecodeError as e:
            logger.error("Invalid JSON message: %s", e)
        except Exception as e:
            logger.exception("Message handling error: %s", e)
    
    def shutdown(self):
        self._shutdown.set()
    
    async def close(self):
        if self._connection:
            await self._connection.close()


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
        self.docker_watcher = DockerWatcher(config, self.stats)
        self.mq_consumer = RabbitMQConsumer(config, self.stats, self.webhook, self.db)
    
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
        
        # RabbitMQ Consumer
        if self.config.rabbitmq.enabled:
            logger.info("RabbitMQ enabled: %s queue=%s", 
                       self.config.rabbitmq.host, 
                       self.config.rabbitmq.queue_name)
            tasks.append(asyncio.create_task(self.mq_consumer.start(), name="mq-consumer"))
        else:
            logger.info("RabbitMQ disabled")
        
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
        self.mq_consumer.shutdown()

        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

        await self.docker_watcher.close()
        await self.mq_consumer.close()
        await self.webhook.close()
        await self.db.close()

        s = self.stats
        logger.info(
            "Stats: docker(recv=%d match=%d filter=%d) mq(recv=%d ok=%d fail=%d) db(ok=%d fail=%d)",
            s.docker_events_received, s.docker_events_matched, s.docker_events_filtered,
            s.mq_messages_received, s.mq_webhook_success, s.mq_webhook_failed,
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
