# 本機 Airflow（Docker Compose）

使用 Apache Airflow **官方** `docker-compose.yaml`（[2.10.5 文件](https://airflow.apache.org/docs/apache-airflow/2.10.5/howto/docker-compose/index.html)），在 Docker Desktop 上跑一套 CeleryExecutor 本機環境。

## 版本為什麼是 2.10.5 + Python 3.8

| 項目 | 版本 | 說明 |
|---|---|---|
| Python | 3.8（`>=3.8.1,<3.9`） | 本專案指定的執行環境 |
| Apache Airflow | **2.10.5** | 仍支援 Python 3.8 的最後一個正式版。2.11 / 3.x 已移除 3.8 |
| 官方映像 | `apache/airflow:2.10.5-python3.8` | 在 `.env` 的 `AIRFLOW_IMAGE_NAME` 鎖定，避免 `latest` 漂到 3.x |

套件版本由 repo 根目錄的 **uv** 管理：`pyproject.toml` + `uv.lock`。Docker 映像本身已含 Airflow 2.10.5；uv 用來對齊本機開發／IDE 與 DAG 依賴。

## 服務規格

官方 compose 是 **CeleryExecutor**：Postgres 當 metadata DB、Redis 當 broker、獨立 webserver / scheduler / worker / triggerer。

**僅供本機開發，不要當 production。**

| 服務 | 角色 | 映像 / 指令 | 連接埠 | 何時啟動 |
|---|---|---|---|---|
| `postgres` | Airflow metadata DB | `postgres:13` | 容器內 `5432`（不對外） | 預設 |
| `redis` | Celery broker | `redis:7.2-bookworm` | 容器內 `6379`（`expose`，不對外） | 預設 |
| `airflow-init` | 建目錄、migrate DB、建立 admin | 一次性，成功後結束 | — | 其他 Airflow 服務會等它完成 |
| `airflow-webserver` | UI / REST | `webserver` | **8080** → http://localhost:8080 | 預設 |
| `airflow-scheduler` | 排程、DAG parse | `scheduler` | 健康檢查 `8974`（容器內） | 預設 |
| `airflow-worker` | Celery 執行 task | `celery worker` | — | 預設 |
| `airflow-triggerer` | deferrable / triggerer | `triggerer` | — | 預設 |
| `airflow-cli` | 進容器跑 `airflow` CLI | profile `debug` | — | `docker compose --profile debug run --rm airflow-cli ...` |
| `flower` | Celery 監控 UI | profile `flower` | **5555** | `docker compose --profile flower up -d` |

### 本機預設帳號

| 項目 | 值 |
|---|---|
| UI | http://localhost:8080 |
| 使用者 | `airflow`（`_AIRFLOW_WWW_USER_USERNAME`） |
| 密碼 | `airflow`（`_AIRFLOW_WWW_USER_PASSWORD`） |
| Executor | `CeleryExecutor` |
| 範例 DAG | 開啟（compose 內 `AIRFLOW__CORE__LOAD_EXAMPLES: 'true'`）。本專案 DAG 在 `dags/` |

### Volume（`AIRFLOW_PROJ_DIR=../..`，即 repo 根目錄）

| 主機路徑 | 容器路徑 |
|---|---|
| `dags/` | `/opt/airflow/dags` |
| `logs/` | `/opt/airflow/logs` |
| `config/` | `/opt/airflow/config` |
| `plugins/` | `/opt/airflow/plugins` |
| Docker volume `postgres-db-volume` | Postgres 資料 |

### Docker Desktop 資源（官方建議）

- 記憶體 ≥ **4 GB**
- CPU ≥ **2**
- 磁碟 ≥ **10 GB** 可用空間

macOS 上 `AIRFLOW_UID` 設成本機 UID（此機為 `501`），避免 `logs/` 變成 root 所有。

## 前置

1. Docker Desktop 已啟動。
2. 本機已安裝 [uv](https://docs.astral.sh/uv/)（管理 Python 3.8 與套件鎖定）：

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

## 用 uv 對齊本機 Python / 套件

在 **repo 根目錄**：

```bash
uv python install 3.8
uv sync
```

這會依 `uv.lock` 建立 `.venv`（Python 3.8 + Airflow 2.10.5 + DAG 依賴：`pandas`、`pymysql`、`pyodbc`、`slack-sdk`、`google-cloud-bigquery` 等）。

之後加套件（只改 uv，不要手改 `uv.lock`）：

```bash
uv add <package>
```

這會更新 `pyproject.toml` 與 `uv.lock`，套件進本機 `.venv`。**不會**自動進 Docker 容器。

### `requirements.txt` 是給容器用的，不是 uv 的來源

`airflow.deployment/docker-compose/requirements.txt` 給官方映像**額外** `pip install`。官方映像已經有 Airflow 2.10.5，不要把 `apache-airflow` 再寫進去，也不要把 `uv export` 的整份 lock 倒進去。

| 檔案 | 誰讀 | 加套件時 |
|---|---|---|
| `pyproject.toml` / `uv.lock` | uv、本機 `.venv`、IDE | 一定 `uv add` |
| `docker-compose/requirements.txt` | 容器 pip | 只有 DAG 在容器裡也要、且映像沒有的套件，才把 `uv.lock` 裡的 `package==version` 抄一行過來 |

例如本機與容器都要 `pyodbc`：

```bash
uv add pyodbc          # 本機
# 再把 uv.lock 的版本寫進 requirements.txt，例如 pyodbc==5.2.0
```

讓容器裝這個檔（每次 `up` 都會 pip，只適合作本機試）：在 `.env` 加上

```bash
_PIP_ADDITIONAL_REQUIREMENTS=-r /opt/airflow/requirements.txt
```

並在 `docker-compose.yaml` 的 `volumes` 掛上 `./requirements.txt:/opt/airflow/requirements.txt`。長期使用請改走 `Dockerfile` + `build: .`（官方也比較建議）。

Airflow 官方建議安裝時帶 constraint。若要重鎖並貼近官方測試組合：

```bash
uv lock --upgrade-package apache-airflow
```

constraint 檔：<https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.8.txt>

## 啟動 Airflow

```bash
cd airflow.deployment/docker-compose

# 第一次會拉映像、跑 airflow-init（migrate + 建立使用者），再起 webserver / scheduler / worker / triggerer
docker compose up -d
```

看狀態與 log：

```bash
docker compose ps
docker compose logs -f airflow-webserver
```

`airflow-init` 顯示 `airflow version` 且狀態為 exited 0 後，打開 http://localhost:8080 ，帳密 `airflow` / `airflow`。

本專案 DAG 在 `dags/mysql_archive_jobs/`。UI 裡新 DAG 預設是 paused（`DAGS_ARE_PAUSED_AT_CREATION`）。

CLI 範例：

```bash
docker compose --profile debug run --rm airflow-cli airflow dags list
```

## 停止 / 清資料

```bash
# 停容器，保留 Postgres volume 與 logs
docker compose down

# 連 metadata DB 一起刪（下次 up 會重新 init）
docker compose down --volumes --remove-orphans
```

## 自訂映像（可選）

同目錄 `Dockerfile` 是官方建議的延伸方式：`FROM apache/airflow:2.10.5-python3.8`。

需要 OS 套件（例如 `pyodbc` 的 unixODBC）或把 uv 鎖定的 extra 打進映像時：

1. 編輯 `Dockerfile`，在 `FROM` 之後加 `RUN` / `pip install`
2. 在 `docker-compose.yaml` 註解 `image:`、打開 `build: .`
3. `docker compose build && docker compose up -d`

臨時試套件（每次啟動都會再裝一次，不適合作長期用）可設 `.env`：

```bash
_PIP_ADDITIONAL_REQUIREMENTS=pyodbc pymysql
```

## 相關文件

- [Running Airflow in Docker（2.10.5）](https://airflow.apache.org/docs/apache-airflow/2.10.5/howto/docker-compose/index.html)
- [Supported versions](https://airflow.apache.org/docs/apache-airflow/2.10.5/installation/supported-versions.html)
- DAG 說明：`dags/mysql_archive_jobs/README.md`
