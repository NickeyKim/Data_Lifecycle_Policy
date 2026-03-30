# Data Lifecycle Policy

Unity Catalog 내 90일 이상 쓰기 작업이 없는 오래된 테이블을 자동으로 식별하고 삭제(TRUNCATE)하는 Databricks Asset Bundle 프로젝트입니다. DRY_RUN 모드와 제외 목록을 통해 안전하게 운영할 수 있습니다.

> **[English version](README.md)**

## Overview

지정된 Unity Catalog 카탈로그 내의 Managed 테이블을 스캔하고, 각 테이블의 Delta 히스토리에서 마지막 쓰기 작업을 확인하여 90일 이상 쓰기가 없는 테이블을 식별합니다. 두 가지 실행 모드를 지원합니다:

- **DRY_RUN** (기본값): 오래된 테이블 후보를 스캔하고 로그만 기록 (변경 없음)
- **TRUNCATE**: 식별된 오래된 테이블을 실제로 삭제

모든 스캔 결과와 실행 내역은 거버넌스 테이블에 기록되어 감사 추적이 가능합니다.

## Architecture

```
system.information_schema.tables
         │
         ▼
  ┌──────────────┐     ┌──────────────────────┐
  │  Managed      │────▶│  DESCRIBE HISTORY     │
  │  테이블 목록   │     │  (테이블별 조회)       │
  └──────────────┘     └──────────┬───────────┘
                                  │
                    마지막 쓰기가 90일 이상 경과?
                                  │
                    ┌─────────────┼─────────────┐
                    ▼             ▼              ▼
              ┌──────────┐ ┌──────────┐  ┌────────────┐
              │  SKIP    │ │ DRY_RUN  │  │  TRUNCATE  │
              │ (제외 대상)│ │ (로그만  │  │  (실행 &   │
              │          │ │  기록)   │  │   로그)     │
              └──────────┘ └──────────┘  └────────────┘
                    │             │              │
                    └─────────────┼──────────────┘
                                  ▼
                     ┌────────────────────────┐
                     │  lifecycle_candidates  │
                     │  (감사 로그 테이블)      │
                     └────────────────────────┘
```

## Job 설정

Job은 **for_each_task** 패턴을 사용하여 여러 타겟 카탈로그를 병렬로 처리합니다.

| 파라미터 | 기본값 | 설명 |
|---------|--------|------|
| `TARGET_CATALOGS` | *(필수)* | 스캔할 카탈로그 이름 (`for_each_task` inputs으로 전달) |
| `RUN_MODE` | `DRY_RUN` | `DRY_RUN`: 로그만 기록, `TRUNCATE`: 실제 데이터 삭제 |

### 오래된 테이블 판별 기준

- 마지막 쓰기 작업(`WRITE`, `MERGE`, `UPDATE`, `DELETE`, `TRUNCATE`, `REPLACE TABLE AS SELECT`)이 **90일 이상 경과**한 테이블
- `__`로 시작하는 테이블은 스캔 대상에서 제외
- `information_schema`, `default`, `_data_classification` 스키마는 제외

## 거버넌스 테이블

### `lifecycle_candidates` (감사 로그)

모든 스캔 결과와 계획/실행된 작업을 기록합니다.

| Column | Type | 설명 |
|--------|------|------|
| `run_id` | STRING | 고유 실행 식별자 (타임스탬프 기반) |
| `scanned_at` | TIMESTAMP | 스캔 실행 시각 |
| `fqtn` | STRING | 정규화된 테이블 이름 |
| `last_write_ts` | TIMESTAMP | Delta 히스토리 기반 마지막 쓰기 시각 |
| `last_op` | STRING | 마지막 쓰기 작업 유형 |
| `is_stale_90d` | BOOLEAN | 90일 이상 경과 여부 |
| `action_planned` | STRING | 계획된 작업: `DRY_RUN` / `TRUNCATE` / `SKIP` |
| `action_executed` | STRING | 실행 결과: `SUCCESS` / `FAILED` / `SKIPPED` |
| `message` | STRING | 추가 컨텍스트 또는 오류 상세 |

### `lifecycle_exclusions` (테이블 단위 제외 목록)

여기에 등록된 테이블은 오래되었더라도 항상 건너뜁니다.

| Column | Type | 설명 |
|--------|------|------|
| `table_catalog` | STRING | 카탈로그 이름 |
| `table_schema` | STRING | 스키마 이름 |
| `table_name` | STRING | 테이블 이름 |
| `reason` | STRING | 제외 사유 |
| `updated_at` | TIMESTAMP | 제외 등록 시각 |

### `lifecycle_exclusions_schema` (스키마 단위 제외 목록)

스캔에서 제외할 전체 스키마를 등록합니다.

| Column | Type | 설명 |
|--------|------|------|
| `table_catalog` | STRING | 카탈로그 이름 |
| `table_schema` | STRING | 스키마 이름 |
| `reason` | STRING | 제외 사유 |
| `updated_at` | TIMESTAMP | 제외 등록 시각 |

## 프로젝트 구조

```
data_lifecycle_policy/
├── databricks.yml                              # DAB 번들 설정 (dev/prod 타겟)
├── resources/
│   └── data_lifecycle_policy.job.yml           # Job 정의 (for_each_task 기반 카탈로그 병렬 처리)
├── src/
│   └── Data Lifecycle Policy.py                # 메인 라이프사이클 정책 노트북
└── scratch/
    └── exploration.ipynb                       # 탐색용 노트북
```

## 시작하기

### 사전 요구사항

- [Databricks CLI](https://docs.databricks.com/dev-tools/cli/databricks-cli.html) v0.18+
- Unity Catalog가 활성화된 Databricks Workspace
- `system.information_schema.tables`에 대한 읽기 권한
- 대상 테이블에 대한 `DESCRIBE HISTORY` 및 `TRUNCATE TABLE` 실행 권한

### 설정

1. Databricks CLI 인증 설정:
   ```bash
   databricks configure
   ```

2. `databricks.yml`에서 workspace host를 본인 환경에 맞게 수정:
   ```yaml
   workspace:
     host: https://<your-workspace>.cloud.databricks.com
   ```

3. 노트북 내 설정을 본인 환경에 맞게 수정:
   - `CANDIDATE_TABLE` — 스캔 결과 저장 테이블
   - `EXCLUSION_TABLE` — 제외 목록 테이블
   - `EXCLUDE_SCHEMAS` — 제외할 스키마 목록
   - `data_lifecycle_policy.job.yml`의 `for_each_task` inputs — 대상 카탈로그 목록

### 배포 및 실행

```bash
# Development 배포
databricks bundle deploy --target dev

# DRY_RUN 모드로 실행 (기본값, 안전)
databricks bundle run data_lifecycle_policy --target dev

# TRUNCATE 모드로 실행 (주의 — 실제 데이터 삭제)
# Job 설정에서 RUN_MODE 파라미터를 "TRUNCATE"로 변경
```

## 안전 기능

- **기본 DRY_RUN 모드**: `RUN_MODE`를 명시적으로 `TRUNCATE`로 설정하지 않으면 데이터가 삭제되지 않음
- **테이블 단위 제외 목록**: 특정 테이블을 삭제 대상에서 영구 보호
- **스키마 단위 제외 목록**: 전체 스키마를 스캔 대상에서 제외
- **전체 감사 추적**: 모든 스캔 및 실행 결과가 `lifecycle_candidates`에 기록
- **시스템 테이블 자동 필터링**: 내부 카탈로그 및 시스템 스키마는 자동으로 제외

## 라이선스

이 프로젝트는 MIT 라이선스를 따릅니다 - [LICENSE](LICENSE) 파일을 참조하세요.

## 참고사항

- Databricks Serverless Notebook 환경에서 테스트되었습니다
- `DESCRIBE HISTORY`를 사용하여 마지막 쓰기 시각을 확인합니다; 쓰기 작업이 없는 경우 마지막 커밋 타임스탬프를 대체로 사용합니다
- Job은 `for_each_task`를 사용하여 여러 카탈로그를 병렬로 처리합니다
