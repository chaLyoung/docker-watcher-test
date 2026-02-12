"""
PostgreSQL 데이터베이스 모듈
"""
import asyncio
import logging
from typing import Optional

import asyncpg

from app.config import PostgresConfig

logger = logging.getLogger("watcher")


class Database:
    """PostgreSQL 비동기 데이터베이스 클라이언트"""

    def __init__(self, config: PostgresConfig):
        self.config = config
        self._pool: Optional[asyncpg.Pool] = None


    @property
    def is_connected(self) -> bool:
        return self._pool is not None and not self._pool._closed


    async def connect(self, max_retries: int = 5, base_delay: float = 2.0):
        """
        커넥션 풀 생성 (재시도 포함)

        Raises:
            ConnectionError: max_retries 초과 시
        """
        for attempt in range(1, max_retries + 1):
            try:
                self._pool = await asyncpg.create_pool(
                    host=self.config.host,
                    port=self.config.port,
                    database=self.config.database,
                    user=self.config.username,
                    password=self.config.password,
                    min_size=2,
                    max_size=10,
                )
                # 실제 쿼리로 접속 검증
                async with self._pool.acquire() as conn:
                    await conn.fetchval("SELECT 1")

                logger.info(
                    "Database connected: %s:%d/%s",
                    self.config.host, self.config.port, self.config.database,
                )
                return

            except Exception as e:
                delay = base_delay * attempt
                logger.error(
                    "Database connection failed (attempt %d/%d): %s (retry in %.1fs)",
                    attempt, max_retries, e, delay,
                )
                if self._pool:
                    await self._pool.close()
                    self._pool = None

                if attempt < max_retries:
                    await asyncio.sleep(delay)

        raise ConnectionError(
            f"Database connection failed after {max_retries} attempts: "
            f"{self.config.host}:{self.config.port}/{self.config.database}"
        )


    async def _reconnect(self):
        """커넥션 풀 재생성"""
        logger.info("Attempting database reconnection...")
        if self._pool:
            try:
                await self._pool.close()
            except Exception:
                pass
            self._pool = None
        await self.connect()


    async def close(self):
        """커넥션 풀 종료"""
        if self._pool:
            await self._pool.close()
            logger.info("Database connection closed")


    async def insert_analysis_history(
        self,
        analysis_type: str,
        req_id: str,
        status: str,
        publish_time=None,
        max_retries: int = 3,
    ) -> int:
        query = """
            INSERT INTO analysis_history (analysis_type, req_id, status, publish_time)
            VALUES ($1, $2, $3, $4)
            RETURNING analysis_history_seq
        """

        # 연결 확인 (최대 max_retries번 재연결 시도)
        for attempt in range(1, max_retries + 1):
            if self.is_connected:
                break
            try:
                await self._reconnect()
            except ConnectionError:
                if attempt == max_retries:
                    raise ConnectionError(f"Reconnect failed after {max_retries} attempts")

        if not self.is_connected:
            raise ConnectionError("Database not connected")

        # 쿼리 실행
        seq = await self._pool.fetchval(query, analysis_type, req_id, status, publish_time)
        logger.info(
            "Inserted analysis_history: seq=%d type=%s req_id=%s status=%s publish_time=%s",
            seq, analysis_type, req_id, status, str(publish_time),
        )
        return seq


    async def update_analysis_history(
        self,
        seq: int,
        status: str,
        max_retries: int = 3,
    ) -> bool:
        """
        analysis_history 테이블 UPDATE (end_time, status)

        Returns:
            성공 여부
        """
        query = """
            UPDATE analysis_history
            SET end_time = NOW(), status = $1
            WHERE analysis_history_seq = $2
        """

        # 연결 확인 (최대 max_retries번 재연결 시도)
        for attempt in range(1, max_retries + 1):
            if self.is_connected:
                break
            try:
                await self._reconnect()
            except ConnectionError:
                if attempt == max_retries:
                    logger.error("Reconnect failed after %d attempts", max_retries)
                    return False

        if not self.is_connected:
            logger.error("Database not connected, update skipped: seq=%d", seq)
            return False

        # 쿼리 실행
        try:
            await self._pool.execute(query, status, seq)
            logger.info("Updated analysis_history: seq=%d status=%s", seq, status)
            return True
        except Exception as e:
            logger.error("DB update failed: seq=%d error=%s", seq, e)
            return False