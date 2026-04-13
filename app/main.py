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

import gzip
import psutil
import shutil
from logging.handlers import TimedRotatingFileHandler

from dataclasses import dataclass, field, asdict
from datetime import datetime
from logging.handlers import RotatingFileHandler
from typing import Optional

import aio_pika
import aiodocker
import httpx

from app.config import Config, QueueConfig, WebhookPayload
from app.database import Database
from app.cog_converter import convert_tif_to_cog

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
    # 큐별 처리 시간 통계
    queue_times: dict = field(default_factory=dict)  # 추가

    def record_time(self, queue_name: str, elapsed: float, success: bool):
        """큐별 처리 시간 기록"""
        if queue_name not in self.queue_times:
            self.queue_times[queue_name] = {
                "count": 0, "success": 0, "failed": 0,
                "total_sec": 0.0,
                "min_sec": float("inf"),
                "max_sec": 0.0,
                "times": [],
                "batch_start": datetime.now(),  # 추가
            }
        s = self.queue_times[queue_name]
        s["count"] += 1
        if success:
            s["success"] += 1
        else:
            s["failed"] += 1
        s["total_sec"] += elapsed
        s["min_sec"] = min(s["min_sec"], elapsed)
        s["max_sec"] = max(s["max_sec"], elapsed)
        s["times"].append(elapsed)

    def get_summary(self, queue_name: str) -> str:
        s = self.queue_times.get(queue_name)
        if not s or s["count"] == 0:
            return "no data"

        avg = s["total_sec"] / s["count"]
        times = sorted(s["times"])
        wall_time = (datetime.now() - s["batch_start"]).total_seconds()

        return (
            f"count={s['count']} success={s['success']} failed={s['failed']} | "
            f"avg={avg:.1f}s min={s['min_sec']:.1f}s max={s['max_sec']:.1f}s "
            f"wall_time={wall_time:.1f}s"
        )

    def get_all_summary(self) -> str:
        """전체 큐 통계"""
        lines = []
        for qname in sorted(self.queue_times.keys()):
            lines.append(f"  [{qname}] {self.get_summary(qname)}")
        return "\n".join(lines) if lines else "  no data"


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
                    logger.info("⭕ Webhook success: url=%s status=%d", url, response.status_code)
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
                await asyncio.sleep(1.0)

        logger.error("❌ Webhook failed after %d attempts: url=%s", self.retry_count, url)
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
        concurrency = self.qcfg.concurrency
        semaphore = asyncio.Semaphore(concurrency)

        while not self._shutdown.is_set():
            try:
                connection = await aio_pika.connect_robust(self.config.rabbitmq.url)
                logger.info("[%s] Connected to RabbitMQ", qname)
                backoff = 1.0

                channel = await connection.channel()
                await channel.set_qos(prefetch_count=concurrency)
                queue = await channel.declare_queue(qname, durable=True)

                logger.info("[%s] Consuming (mode=%s, concurrency=%d)", qname, self.qcfg.mode, concurrency)

                async def on_message(message: aio_pika.IncomingMessage):
                    async with semaphore:
                        async with message.process():
                            await self._handle_message(message)

                await queue.consume(on_message)

                # shutdown 대기
                while not self._shutdown.is_set():
                    await asyncio.sleep(1.0)

            except asyncio.CancelledError:
                break
            except Exception as e:
                if self._shutdown.is_set():
                    break

                logger.error("[%s] RabbitMQ error: %s (retry in %.1fs)", qname, e, backoff)

                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30.0)

    async def _handle_message(self, message: aio_pika.IncomingMessage):
        """메시지 처리: DB INSERT → 분석 실행 → DB UPDATE → Webhook"""
        qname = self.qcfg.name
        self.stats.mq_messages_received += 1
        msg_start_time = datetime.now()  # 추가: 시작 시간
        seq = None
        is_success = True
        error_message = None
        save_path = ""
        elapsed = None

        proc = psutil.Process()
        proc.cpu_percent()          # 첫 호출: 기준점만 잡음
        psutil.cpu_percent()        # 시스템도 기준점

        try:
            body = json.loads(message.body.decode())

            if isinstance(body, str):
                body = json.loads(body)

            # MQ 메시지 전체 로깅 (pretty)
            logger.info("✅ [%s] MQ body:\n%s", qname, json.dumps(body, indent=2, ensure_ascii=False))

            # ── 공통 필드 (모든 큐 공통) ──
            analysis_type = body.get("type", "")
            geospatial_analysis_type = body.get("geospatialAnalysisType", "")
            callback_url = body.get("callbackUrl", "")
            request_id = body.get("requestId", "")
            request_date_time = body.get("requestDateTime", "")
            publish_time = datetime.fromtimestamp(int(request_date_time) / 1000) if request_date_time else None

            # container 모드 Webhook용
            request_user_id = body.get("requestUserId")
            battalion_phase_id = body.get("battalionPhaseId")
            battalion_aspect_id = body.get("battalionAspectId")

            if not request_id:
                alphabet = string.ascii_lowercase + string.digits
                su = shortuuid.ShortUUID(alphabet=alphabet)
                request_id = str(su.random(length=8))
                logger.warning("[%s] Generated request_id: %s", qname, request_id)

            # ── 공통 검증: callbackUrl만 필수 ──
            if not callback_url:
                logger.warning("[%s] No callbackUrl, skipping", qname)
                return

            # ── 작업 디렉토리 구성 (container 모드용) ──
            yyyymm = datetime.now().strftime("%Y%m")
            work_dir_host = os.path.join(self.config.spatial_data_path, 'result', yyyymm, request_id)
            # work_dir_container = os.path.join(self.config.spatial_mount_path, 'result', yyyymm, request_id)

            # ── 1) DB INSERT ──
            try:
                seq = await self.db.insert_analysis_history(
                    analysis_type=analysis_type,
                    request_id=request_id,
                    request_user_id=request_user_id,
                    battalion_phase_id=battalion_phase_id,
                    battalion_aspect_id=battalion_aspect_id,
                    request_date_time=publish_time,
                    status="10",
                )
                logger.info("[%s] DB insert success: seq=%d", qname, seq)
            except Exception as e:
                logger.error("[%s] DB insert failed: %s", qname, e)

            # ── 2) 분석 실행 (모드별 분기) ──
            try:
                if self.qcfg.mode == "container":
                    logger.info("[%s] Running container mode: image=%s work_dir=%s",
                                qname, self.qcfg.image, work_dir_host)
                    is_success, error_message = await self._run_container(body, request_id, work_dir_host)
                else:
                    # Service 모드: 파일 경로 검증
                    opord_path = body.get("opordPath", "").replace("\\", "/")
                    full_path = os.path.join(self.config.data_root_path, opord_path)
                    output_path = os.path.dirname(full_path).replace("/input", "/output")
                    save_path = os.path.join(output_path, request_id)
                    os.makedirs(save_path, exist_ok=True)

                    logger.info("[%s] Running service mode: url=%s full_path=%s save_path=%s",
                                qname, self.qcfg.service_url, full_path, save_path)

                    if not os.path.exists(full_path):
                        is_success = False
                        error_message = "File not found: " + full_path
                        logger.warning("[%s] %s", qname, error_message)
                    else:
                        logger.info("[%s] File verified: %s", qname, full_path)
                        is_success, error_message = await self._call_service(full_path, request_id, save_path)
            except Exception as e:
                is_success = False
                error_message = f"Analysis error: {e}"
                logger.exception("[%s] %s", qname, error_message)

            if is_success:
                self.stats.analysis_success += 1
            else:
                self.stats.analysis_failed += 1

            # ── 3) DB UPDATE ──
            if seq is not None:
                status = "20" if is_success else "99"
                try:
                    await self.db.update_analysis_history(seq=seq, status=status)
                    logger.info("[%s] DB update: seq=%d status=%s", qname, seq, status)
                except Exception as e:
                    logger.error("[%s] DB update failed: %s", qname, e)

            # ── 4) Webhook 전송 ──

            elapsed = (datetime.now() - msg_start_time).total_seconds()

            # 시스템 리소스 수집 (시작 시점 대비 사용량)
            sys_cpu = psutil.cpu_percent()        # 시작 이후 CPU 사용률
            sys_mem = psutil.virtual_memory()
            proc_cpu = proc.cpu_percent()         # 시작 이후 프로세스 CPU
            proc_mem = proc.memory_info()

            system_stats = {
                "cpuCount": psutil.cpu_count(),
                "cpuPercent": psutil.cpu_percent(interval=None),
                "memTotalGb": round(sys_mem.total / (1024**3), 1),
                "memUsedGb": round(sys_mem.used / (1024**3), 1),
                "memPercent": sys_mem.percent,
                "watcherCpuPercent": proc.cpu_percent(interval=None),
                "watcherMemMb": round(proc_mem.rss / (1024**2), 1),
                "watcherMemPercent": round(proc.memory_percent(), 1),
            }

            if self.qcfg.mode == "service":
                # Service 모드: 기존 포맷
                save_path = save_path.replace(self.config.data_root_path, "")
                payload = WebhookPayload(
                    success=is_success,
                    message=error_message,
                    requestId=request_id,
                    situationId=body.get("situationId", ""),
                    brigadePhaseId=body.get("brigadePhaseId", ""),
                    preproccessingPath=os.path.join(save_path, "json", "result.json"),
                )
            else:
                # Container 모드: API 명세서 포맷 (tiff/shape)
                payload = self._build_webhook_payload(
                    is_success=is_success,
                    error_message=error_message,
                    request_id=request_id,
                    request_user_id=request_user_id,
                    battalion_phase_id=battalion_phase_id,
                    battalion_aspect_id=battalion_aspect_id,
                    geospatial_analysis_type=geospatial_analysis_type,
                    work_dir_host=work_dir_host,
                    body=body,
                )

            payload_dict = payload.to_dict()
            payload_dict["processingTime"] = round(elapsed, 2)
            # payload_dict["systemStats"] = system_stats

            logger.info("[%s] Sending webhook: url=%s\n%s", qname, callback_url, json.dumps(payload_dict, indent=2, ensure_ascii=False))
            webhook_result = await self.webhook.send(callback_url, payload_dict)

        except json.JSONDecodeError as e:
            elapsed = (datetime.now() - msg_start_time).total_seconds()
            logger.error("[%s] Invalid JSON: %s", qname, e)
        except Exception as e:
            elapsed = (datetime.now() - msg_start_time).total_seconds()
            logger.exception("[%s] Message handling error: %s", qname, e)
        finally:
            self.stats.record_time(qname, elapsed, is_success)

            logger.info(
                "📊 [%s] Pipeline finished: elapsed=%.2fs success=%s",
                qname, elapsed, is_success,
            )

            # 50건마다 통계 출력
            q_stats = self.stats.queue_times.get(qname, {})
            if q_stats.get("count", 0) % 50 == 0:
                logger.info(
                    "📊 [%s] Stats (every 50): %s",
                    qname, self.stats.get_summary(qname),
                )

    def _build_webhook_payload(
        self,
        is_success: bool,
        error_message: Optional[str],
        request_id: str,
        request_user_id: Optional[str],
        battalion_phase_id: Optional[str],
        battalion_aspect_id: Optional[str],
        geospatial_analysis_type: Optional[str],
        work_dir_host: str,
        body: dict,
    ) -> WebhookPayload:
        """Container 모드: 큐 설정(response_type, result_filename)에 따라 Webhook 페이로드 생성"""
        payload = WebhookPayload(
            success=is_success,
            message=error_message,
            requestId=request_id,
            requestUserId=request_user_id,
            battalionPhaseId=battalion_phase_id,
            battalionAspectId=battalion_aspect_id,
            geospatialAnalysisType=geospatial_analysis_type,
            resultPath=work_dir_host,
        )

        if not is_success:
            return payload

        # result_filename에 {profile} 등 치환
        filename = self.qcfg.result_filename
        if "{profile}" in filename:
            profile = body.get("profile", "pedestrian")
            filename = filename.replace("{profile}", profile)

        result_path = os.path.join(work_dir_host, filename)

        if self.qcfg.response_type == "shape":
            payload.shapePath = result_path
        else:
            # tiff: gisTiffPath + cogTiffPath
            payload.gisTiffPath = result_path
            base, ext = os.path.splitext(result_path)
            payload.cogTiffPath = f"{base}_cog{ext}"
        
        return payload

    # ── Container 모드 ──────────────────────────────────────

    async def _run_container(self, body: dict, request_id: str, work_dir: str) -> tuple[bool, Optional[str]]:
        """Docker 컨테이너 실행 후 종료 대기 → (성공여부, 에러메시지) 반환"""
        qname = self.qcfg.name
        container_name = f"{qname}-{uuid.uuid4().hex[:8]}"
        memory_samples = []
        monitor_task = None
        env_vars = self._build_env_vars(body, work_dir)

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
            # Docker run 명령어 형식으로 로깅
            docker_cmd_lines = ["docker run --rm \\"]
            for bind in self.qcfg.volumes:
                docker_cmd_lines.append(f"  -v {bind} \\")
            for k, v in env_vars.items():
                docker_cmd_lines.append(f"  -e {k}={v} \\")
            if self.qcfg.network:
                docker_cmd_lines.append(f"  --network {self.qcfg.network} \\")
            docker_cmd_lines.append(f"  --name {container_name} \\")
            docker_cmd_lines.append(f"  {self.qcfg.image}")
            logger.info("[%s] Running:\n%s", qname, "\n".join(docker_cmd_lines))

            container = await self.docker.containers.create(
                config=container_config, name=container_name,
            )

            # 출력 디렉토리 미리 생성 (호스트 경로)
            # host_work_dir = work_dir.replace(
            #     self.config.spatial_mount_path,
            #     self.config.spatial_data_path,
            # )

            os.makedirs(work_dir, exist_ok=True)
            logger.info("[%s] Output dir created: %s", qname, work_dir)

            start_time = datetime.now()
            await container.start()
            logger.info("[%s] Container started: %s", qname, container_name)

            # 메모리 모니터링 시작 (공유 리스트에 수집)
            monitor_task = asyncio.create_task(
                self._monitor_container_memory(container, container_name, memory_samples)
            )

            timeout = self.qcfg.timeout or 600
            result = await asyncio.wait_for(container.wait(), timeout=timeout)
            end_time = datetime.now()

            # 모니터링 중지
            monitor_task.cancel()
            try:
                await monitor_task
            except asyncio.CancelledError:
                pass

            exit_code = result.get("StatusCode", -1)
            elapsed = (end_time - start_time).total_seconds()

            logger.info(
                "[%s] Container finished: name=%s exit_code=%d elapsed=%.2fs (start=%s end=%s)",
                qname, container_name, exit_code, elapsed,
                start_time.strftime("%H:%M:%S.%f")[:-3],
                end_time.strftime("%H:%M:%S.%f")[:-3],
            )

            # 1) 컨테이너 종료 코드 확인
            if exit_code != 0:
                # 컨테이너 에러 로그 수집
                try:
                    logs = await container.log(stdout=True, stderr=True)
                    container_logs = "".join(logs).strip()
                    if container_logs:
                        logger.error("❌[%s] Container logs (%s):\n%s", qname, container_name, container_logs[-2000:])
                except Exception:
                    pass
                    
                msg = f"Container exited with code {exit_code}"
                logger.error("[%s] %s", qname, msg)
                return False, msg

            # 2) 산출물 파일 존재 확인
            if self.qcfg.result_filename:
                filename = self.qcfg.result_filename
                if "{profile}" in filename:
                    profile = body.get("profile", "pedestrian")
                    filename = filename.replace("{profile}", profile)

                result_path = os.path.join(work_dir, filename)

                if not os.path.exists(result_path):
                    msg = f"Output file not found: {result_path}"
                    logger.error("[%s] %s", qname, msg)
                    return False, msg

                logger.info("[%s] Output verified: %s", qname, result_path)

                # 2) tiff 타입이면 COG 변환
                if self.qcfg.response_type == "tiff":
                    try:
                        base, ext = os.path.splitext(result_path)
                        cog_path = f"{base}_cog{ext}"
                        convert_tif_to_cog(result_path, cog_path)

                        # COG 파일 존재 확인
                        if not os.path.exists(cog_path):
                            msg = f"COG file not created: {cog_path}"
                            logger.error("[%s] %s", qname, msg)
                            return False, msg

                        logger.info("[%s] COG verified: %s", qname, cog_path)

                    except Exception as e:
                        msg = f"COG conversion failed: {e}"
                        logger.error("[%s] %s", qname, msg)
                        return False, msg

                # 메모리 사용량 파일 저장
            if memory_samples:
                self._save_memory_report(qname, container_name, request_id,
                                         start_time, end_time, memory_samples, work_dir)

            return True, None

        except asyncio.TimeoutError:
            msg = f"Container timeout after {timeout}s: {container_name}"
            logger.error("[%s] %s", qname, msg)
            return False, msg

        except Exception as e:
            msg = f"Container error: {e}"
            logger.error("[%s] %s: name=%s", qname, msg, container_name)
            return False, msg

        finally:
            if monitor_task and not monitor_task.done():
                monitor_task.cancel()
            if container:
                try:
                    await container.kill()
                except Exception:
                    pass
                try:
                    await container.delete(force=True)
                    logger.info("[%s] Container removed: %s", qname, container_name)
                except Exception:
                    pass

    def _build_env_vars(self, body: dict, work_dir: str) -> dict:
        """메시지 본문 + 작업 디렉토리에서 컨테이너 환경변수 생성"""
        env = {}

        # 1) 작업 디렉토리 기반 기본값 (OUTPUT_PATH)
        env["OUTPUT_PATH"] = work_dir

        # 3) 큐별 고정 환경변수 (watcher.yml env)       ← 이 부분 추가
        env.update(self.qcfg.env)

        # 2) 큐별 동적 환경변수 (MQ body → Docker ENV, env_mapping)
        #    MQ 메시지에 값이 있으면 기본값을 덮어씀
        for body_key, env_name in self.qcfg.env_mapping.items():
            value = body.get(body_key)
            if value is not None:
                env[env_name] = str(value)

        # 임시코드 사격선 MODE
        if self.qcfg.analysis_type == 'b0002':
            env["MODE"] = 'ocoka'

        # 3) 시스템 환경변수
        env["ANALYSIS_TYPE"] = self.qcfg.analysis_type

        return env

    # ── Service 모드 ────────────────────────────────────────

    async def _call_service(self, full_path: str, request_id: str, save_path: str) -> tuple[bool, Optional[str]]:
        """HTTP 서비스 호출 → (성공여부, 에러메시지) 반환"""
        qname = self.qcfg.name
        url = self.qcfg.service_url

        try:
            logger.info("[%s] Calling service: %s", qname, url)

            payload = {
                "request_id": request_id,
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
                return True, None

            msg = f"Service error: status={response.status_code} body={response.text[:200]}"
            logger.warning("[%s] %s", qname, msg)
            return False, msg

        except Exception as e:
            msg = f"Service call failed: {e}"
            logger.error("[%s] %s", qname, msg)
            return False, msg


    async def _monitor_container_memory(self, container, container_name: str,
                                         samples: list, interval: float = 1.0):
        """컨테이너 메모리 사용량 주기적 수집 (samples 리스트에 직접 추가)"""
        try:
            while True:
                stats = await container.stats(stream=False)
                if stats:
                    stat = stats[0] if isinstance(stats, list) else stats
                    mem_usage = stat.get("memory_stats", {}).get("usage", 0)
                    mem_limit = stat.get("memory_stats", {}).get("limit", 0)
                    samples.append({
                        "timestamp": datetime.now().strftime("%H:%M:%S.%f")[:-3],
                        "usage_mb": round(mem_usage / 1024 / 1024, 1),
                        "limit_mb": round(mem_limit / 1024 / 1024, 1),
                        "percent": round(mem_usage / mem_limit * 100, 2) if mem_limit > 0 else 0,
                    })
                await asyncio.sleep(interval)
        except asyncio.CancelledError:
            pass
        except Exception:
            pass


    def _save_memory_report(self, qname: str, container_name: str, request_id: str,
                            start_time: datetime, end_time: datetime,
                            samples: list, work_dir: str):
        if not samples:
            return

        usage_list = [s["usage_mb"] for s in samples]
        report = {
            "queue": qname,
            "container": container_name,
            "requestId": request_id,
            "start_time": start_time.strftime("%Y-%m-%d %H:%M:%S"),
            "end_time": end_time.strftime("%Y-%m-%d %H:%M:%S"),
            "elapsed_s": round((end_time - start_time).total_seconds(), 2),
            "samples_count": len(samples),
            "memory_mb": {
                "avg": round(sum(usage_list) / len(usage_list), 1),
                "min": round(min(usage_list), 1),
                "max": round(max(usage_list), 1),
                "limit": samples[0]["limit_mb"],
            },
            "samples": samples,
        }

        # /var/log/aetem/watcher/memory/B0006/AcTdr0ktD7O1_20260403_065700.json
        mem_dir = os.path.join("/var/log/aetem/watcher", "memory", qname)

        os.makedirs(mem_dir, exist_ok=True)
        filename = f"{request_id}_{start_time.strftime('%Y%m%d_%H%M%S')}.json"
        filepath = os.path.join(mem_dir, filename)

        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2, ensure_ascii=False)

        logger.info("[%s] Memory report saved: %s/%s (avg=%.1fMB max=%.1fMB)",
                    qname, qname, filename, report["memory_mb"]["avg"], report["memory_mb"]["max"])


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
            "Stats: docker(recv=%d match=%d filter=%d) mq(recv=%d ok=%d fail=%d)",
            s.docker_events_received, s.docker_events_matched, s.docker_events_filtered,
            s.mq_messages_received, s.analysis_success, s.analysis_failed,
        )

    def shutdown(self):
        self._shutdown.set()


# ============================================================
# 로깅
# ============================================================
def setup_logging(log_dir: str):
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    logger.setLevel(logging.INFO)

    # 콘솔 출력
    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(fmt)
    logger.addHandler(console)

    # 날짜별 로그 파일
    os.makedirs(log_dir, exist_ok=True)
    log_path = os.path.join(log_dir, "aetem-v2-watcher.log")

    file_handler = TimedRotatingFileHandler(
        log_path,
        when="midnight",           # 자정 기준 롤오버
        interval=1,                # 1일 간격
        backupCount=30,            # 최대 30일 보관
        encoding="utf-8",
    )
    file_handler.suffix = "%Y_%m_%d.0.log"              # 롤오버된 파일명 접미사
    file_handler.namer = _log_namer                      # 파일명 형식 커스텀
    file_handler.rotator = _log_rotator                  # gz 압축
    file_handler.setFormatter(fmt)
    logger.addHandler(file_handler)


def _log_namer(name: str) -> str:
    """aetem-v2-watcher.2026_02_02.0.log 형식으로 변환"""
    # name = "/var/log/aetem/watcher/aetem-v2-watcher.log.2026_02_02.0.log"
    # → "/var/log/aetem/watcher/aetem-v2-watcher.2026_02_02.0.log.gz"
    base = name.replace(".log.", ".")
    return base + ".gz"


def _log_rotator(source: str, dest: str):
    """롤오버 시 이전 로그를 gz 압축"""
    with open(source, "rb") as f_in:
        with gzip.open(dest, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)
    os.remove(source)


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
