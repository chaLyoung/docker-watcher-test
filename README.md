# AETEM v2.0 Docker Watcher

RabbitMQ 메시지를 consume하여 분석 Docker 컨테이너를 실행하고, 완료 후 Webhook으로 결과를 전달하는 미들웨어입니다.

## 목차

- [아키텍처](#아키텍처)
- [디렉토리 구조](#디렉토리-구조)
- [설정 파일](#설정-파일)
- [동작 방식](#동작-방식)
- [큐 목록](#큐-목록)
- [배포](#배포)
- [서버 정보](#서버-정보)
- [운영 가이드](#운영-가이드)
- [트러블슈팅](#트러블슈팅)

---

## 아키텍처

```
[Spring API] → [RabbitMQ] → [Watcher] → [Docker Container / HTTP Service]
                                  ↓
                              [PostgreSQL] (분석 이력 저장)
                                  ↓
                              [Webhook] → [Spring API 콜백]
```

### 핵심 흐름

1. Spring API가 RabbitMQ 큐에 분석 요청 메시지 발행
2. Watcher가 해당 큐를 consume
3. DB에 분석 이력 INSERT (status=10, 진행중)
4. 모드에 따라 분석 실행
   - **Container 모드**: Docker 컨테이너 실행 → 종료 대기 → 산출물 검증 → COG 변환(tiff인 경우)
   - **Service 모드**: HTTP POST 요청 → 응답 대기
5. DB UPDATE (status=20 성공 / 99 실패)
6. Webhook으로 Spring API에 결과 전달

---

## 디렉토리 구조

```
/deploy/source/watcher/
├── app/
│   ├── main.py              # 메인 애플리케이션 (Consumer, Watcher, Webhook)
│   ├── config.py            # 설정 클래스 (Config, QueueConfig, WebhookPayload)
│   ├── database.py          # PostgreSQL 연결/쿼리
│   └── cog_converter.py     # GeoTIFF → COG 변환
├── config/
│   └── watcher.yml          # 큐 정의 및 분석 설정
├── docker-compose.yml       # Docker Compose 설정
├── Dockerfile               # Watcher 이미지 빌드
├── requirements.txt         # Python 의존성
└── README.md
```

---

## 설정 파일

### docker-compose.yml

```yaml
services:
  aetem-v2-watcher:
    build: .
    image: aetem-v2-watcher:latest
    container_name: aetem-v2-watcher
    restart: unless-stopped
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock:ro   # Docker API 접근
      - ./config:/config:ro                            # watcher.yml
      - /var/log/aetem/watcher:/var/log/aetem/watcher  # 로그
      - /deploy/data/aetem/app:/deploy/data/aetem/app  # GIS 데이터
    environment:
      - CONFIG_PATH=/config/watcher.yml
      - POSTGRES_HOST=10.0.100.130
      - RABBITMQ_ENABLED=true
      - RABBITMQ_HOST=10.0.100.130
      # ... (전체 환경변수는 docker-compose.yml 참고)
    networks:
      - aetem_v2
```

### watcher.yml 주요 설정

| 필드 | 설명 | 예시 |
|---|---|---|
| `name` | RabbitMQ 큐 이름 | `B0001` |
| `mode` | 실행 모드 | `container` / `service` |
| `image` | Docker 이미지 | `analysis-observation-viewshed:latest` |
| `timeout` | 타임아웃 (초) | `600` |
| `response_type` | 응답 유형 | `tiff` / `shape` |
| `result_filename` | 산출물 파일명 | `viewshed.tif` |
| `result_filename_map` | 모드별 산출물 매핑 | `{ocoka: "fire_classified.tif"}` |
| `concurrency` | 동시 처리 수 | `1` (기본), B0010은 `5` |
| `volumes` | Docker 볼륨 마운트 | `/deploy/data/aetem/app/gis:/deploy/data/aetem/app/gis` |
| `env_mapping` | MQ body → Docker ENV 매핑 | `{observerPositions: OBSERVER_POSITIONS}` |
| `env` | 고정 환경변수 | `{BASE_PATH: /mnt/data/...}` |

### config.py 주요 클래스

- **Config**: 전체 설정 (환경변수 + YAML 로드)
- **QueueConfig**: 개별 큐 설정
- **WebhookPayload**: Webhook 응답 페이로드 (`to_dict()`로 None 필드 자동 제외)
- **RabbitMQConfig**: RabbitMQ 연결 (비밀번호 URL 인코딩 포함)

### 환경변수 우선순위 (_build_env_vars)

```
1. OUTPUT_PATH (자동 생성: /deploy/data/.../result/YYYYMM/requestId)
2. watcher.yml의 env (고정값, 덮어씀)
3. MQ body의 env_mapping (동적값, 덮어씀)
4. ANALYSIS_TYPE (시스템 자동)
```

---

## 동작 방식

### Container 모드

```
MQ 메시지 수신
  → DB INSERT (status=10)
  → Docker 컨테이너 생성 + 시작
  → 메모리 모니터링 시작 (백그라운드)
  → container.wait() (종료 대기)
  → 메모리 모니터링 중지
  → exit_code 확인 (≠0이면 실패, 컨테이너 로그 수집)
  → 로그에서 에러 패턴 감지 (_detect_error_from_logs)
  → 산출물 파일 존재 확인
  → tiff면 COG 변환 (convert_tif_to_cog)
  → 메모리 리포트 저장 (/var/log/aetem/watcher/memory/{큐명}/)
  → DB UPDATE (status=20/99)
  → Webhook 전송
  → 컨테이너 삭제
```

### Service 모드

```
MQ 메시지 수신
  → DB INSERT (status=10)
  → opordPath로 파일 존재 확인
  → HTTP POST 전송 (requestData 있으면 그 내용만, 없으면 {request_id, input_path, output_path})
  → 응답 수신 (responseData)
  → DB UPDATE (status=20/99)
  → Webhook 전송 (responseData 포함)
```

### Concurrency (동시 처리)

- `watcher.yml`의 `concurrency` 설정으로 큐당 동시 처리 수 조절
- `asyncio.Semaphore` + `prefetch_count`로 제어
- B0010(이동로)만 `concurrency: 5`, 나머지는 기본 `1`
- concurrency=6 이상은 메모리 급등으로 비권장

### 에러 코드 체계

| 코드 | 의미 |
|---|---|
| `D001` | Docker 컨테이너 exit code ≠ 0 |
| `D002` | Docker 컨테이너 타임아웃 |
| `D003` | Docker 기타 에러 |
| `W001` | 산출물 파일 미생성 |
| `W002` | COG 변환 실패 |
| `W003` | COG 파일 미생성 |
| `W999` | Watcher 일반 에러 |
| `L001` | 경로 탐색 실패 (분석 모듈) |
| `L002` | 메모리 부족 (분석 모듈) |
| `L003` | 파일 미존재 (분석 모듈) |
| `L004` | 권한 거부 (분석 모듈) |

---

## 큐 목록

### Service 모드

| 큐 | 분석명 | URL |
|---|---|---|
| A0001 | OPORD 전처리 | `http://opord-preprocess:8100/summarizer` |
| A0004 | 사격 할당 | `http://opord-preprocess:8100/test` |

### Container 모드 (분석)

| 큐 | 분석명 | 이미지 | 응답 | 산출물 |
|---|---|---|---|---|
| B0001 | 가시권분석 | analysis-observation-viewshed | tiff | visibility_heatmap.tif |
| B0002 | 사격선(직사) | analysis-observation-direct | tiff | direct_fire.tif / fire_classified.tif |
| B0003 | 사격선(곡사) | analysis-observation-throwshed | tiff | indirect_fire.tif |
| B0004 | 접근로분석 | analysis-aoa | shape | aoa_routes.zip |
| B0005 | 핵심지형 | analysis-keyterrain | shape | key_terrain.zip |
| B0006 | 장애지형 | analysis-obstacle | tiff | obstacle_heatmap.tif |
| B0007 | 은폐분석 | analysis-concealment | tiff | concealment_heatmap.tif |
| B0008 | 엄폐분석 | analysis-cover | tiff | cover_heatmap.tif |
| B0009 | 진출선분석 | analysis-movement-timeline | shape | timeline_{profile}.zip |
| B0010 | 이동로분석 | analysis-pathfinding | shape | path_{profile}.zip |

### Container 모드 (전처리)

| 큐 | 분석명 | 이미지 | timeout | 산출물 |
|---|---|---|---|---|
| B1001 | 데이터 변환 | analysis-convert-data | 1800s | veg_valid.tif |
| B1002 | 비용맵 생성 | analysis-costmap | 1200s | cost_tracked.tif |
| B1003 | 감제고지 | analysis-commanding-heights | 1200s | selected_ops.zip |
| B1004 | LAS→3DTiles | analysis-convert-las | 1800s | tileset.json |
| B1005 | DEM→Terrain | analysis-convert-dem | 3600s | layer.json |

---

## 배포

### Jenkins 배포 (기본)

```bash
cd /deploy/source/watcher
docker compose down
docker compose up --build -d
```

### 수동 배포

```bash
cd /deploy/source/watcher
docker compose down
docker compose up --build -d
docker logs -f aetem-v2-watcher
```

### watcher.yml만 수정한 경우

watcher.yml은 `./config:/config:ro`로 마운트되어 있으므로 watcher 재시작만 하면 됩니다.

```bash
docker compose restart
```

### 분석 Docker 이미지 배포 (Jenkins)

각 분석 모듈은 별도 Jenkins item으로 빌드합니다. 동시 빌드 충돌 방지를 위해 item별 경로가 분리되어 있습니다.

```bash
# Jenkins Exec command 예시
set -e
BASE=/deploy/source/spatial_analysis_modules/${JOB_NAME}
cd "$BASE/analysis"
docker build --no-cache -f docker/{모듈명}/Dockerfile -t analysis-{모듈명}:latest .
rm -rf $BASE/analysis/*
```

---

## 서버 정보

### 130 서버 (개발/운영)

| 항목 | 값 |
|---|---|
| IP | 10.0.100.130 |
| CPU | 6코어 |
| RAM | 125.1GB |
| PostgreSQL | 10.0.100.130:5432 (aetemdb) |
| RabbitMQ | 10.0.100.130:5672 |
| RabbitMQ 관리 | http://10.0.100.130:15672 |
| GIS 데이터 | /deploy/data/aetem/app/gis |

### NAS 마운트

```bash
# /etc/fstab에 등록 (서버 재부팅 시 자동 마운트)
10.200.100.13:/volume1/dms/aetem_v2 /deploy/data/aetem/app nfs4 defaults,hard,timeo=600 0 0
```

### Docker 자동 시작

```bash
sudo systemctl enable docker  # Docker 데몬 자동 시작
# restart: unless-stopped → 컨테이너 자동 시작 (docker compose down 하면 안됨)
```

---

## 운영 가이드

### 로그 확인

```bash
# 실시간 로그
docker logs -f aetem-v2-watcher

# 로그 파일 (날짜별 자동 롤오버, gz 압축, 30일 보관)
ls /var/log/aetem/watcher/
cat /var/log/aetem/watcher/aetem-v2-watcher.log

# 압축된 과거 로그
zcat /var/log/aetem/watcher/aetem-v2-watcher.2026_04_10.0.log.gz
zgrep "B0006" /var/log/aetem/watcher/aetem-v2-watcher.2026_04_10.0.log.gz
```

### 컨테이너 메모리 리포트

```bash
# 분석 컨테이너별 메모리 사용량 (1초 간격 샘플링)
ls /var/log/aetem/watcher/memory/B0006/
cat /var/log/aetem/watcher/memory/B0006/{requestId}_{timestamp}.json
```

### RabbitMQ 큐 상태 확인

```bash
# 큐에 쌓인 메시지 수
docker exec rabbitmq rabbitmqctl list_queues name messages

# 관리 UI
http://10.0.100.130:15672  (aistudio / aistudio1@#$)
```

### DB 분석 이력 확인

```sql
-- 최근 분석 이력
SELECT * FROM analysis_history ORDER BY analysis_history_seq DESC LIMIT 20;

-- 실패 건 조회
SELECT * FROM analysis_history WHERE analysis_status = '99' ORDER BY start_time DESC;

-- 상태값: 10=진행중, 20=성공, 99=실패
```

### 성능 테스트

```bash
# callback-server 실행 (테스트용)
cd /deploy/source/watcher/callback_server
docker compose up -d
# 대시보드: http://<서버IP>:9001

# 테스트 실행 (서버에서)
python test_performance.py --queue B0006 --count 10
python test_performance.py --all --count 50 --reset
```

---

## 트러블슈팅

### Watcher가 안 뜰 때

```bash
# 컨테이너 상태 확인
docker ps -a | grep watcher

# 로그 확인
docker logs aetem-v2-watcher --tail 50

# 일반적인 원인:
# - RabbitMQ 연결 실패 → RABBITMQ_HOST 확인
# - DB 연결 실패 → POSTGRES_HOST 확인
# - NAS 마운트 해제 → mount -a 실행
```

### 분석 컨테이너 에러

```bash
# 실패한 요청의 로그에서 container logs 확인
docker logs aetem-v2-watcher | grep "Container logs"

# 수동으로 분석 컨테이너 실행 테스트
docker run --rm \
  -v /deploy/data/aetem/app/gis:/deploy/data/aetem/app/gis \
  -e OUTPUT_PATH=/tmp/test \
  -e GIS_DATA_PATH=/deploy/data/aetem/app/gis/pyeongchang \
  analysis-observation-viewshed:latest
```

### NAS 마운트 풀렸을 때

```bash
df -h | grep aetem    # 마운트 상태 확인
sudo mount -a         # fstab 기준 재마운트
```

### Webhook SSL 에러

Watcher의 httpx 클라이언트는 `verify=False`로 설정되어 있지 않습니다. 자체 서명 인증서 사용 시 `WebhookSender`의 `_client` 생성 부분에 `verify=False` 추가가 필요할 수 있습니다.

### 서버 재시작 후 체크리스트

1. NAS 마운트 확인: `df -h | grep aetem`
2. Docker 데몬 확인: `systemctl status docker`
3. Watcher 컨테이너 확인: `docker ps | grep watcher`
4. RabbitMQ 연결 확인: `docker logs aetem-v2-watcher --tail 20`

---

## 주요 의존성

| 패키지 | 용도 | 라이선스 |
|---|---|---|
| aio-pika | RabbitMQ async 클라이언트 | Apache-2.0 |
| aiodocker | Docker API async 클라이언트 | Apache-2.0 |
| httpx | HTTP 클라이언트 (Webhook, Service 호출) | BSD-3 |
| psutil | 시스템 리소스 모니터링 | BSD-3 |
| asyncpg | PostgreSQL async 드라이버 | Apache-2.0 |
| PyYAML | watcher.yml 파싱 | MIT |
| shortuuid | requestId 자동 생성 | BSD-3 |