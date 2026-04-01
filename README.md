# Data Ingestion Service

FastAPI service for chunked CSV ingestion (stores, users, store–user mappings) per the assignment HLD: async SQLAlchemy + PostgreSQL, background processing, strict CSV headers, and job polling.

## Documentation References

You can refer to the required high-level design in [high_level_design.md](./high_level_design.md).

Detailed metrics and ingestion stats can be found in [performance_evidence.md](./performance_evidence.md).


## Prerequisites

- Python 3.11+
- Docker (optional, for PostgreSQL)

## Configuration

Copy the template and edit locally ([`sample.env`](sample.env) → `.env` at the repo root). Settings load from `data_ingestion.config`: either **`DATABASE_URL`** or the split variables **`POSTGRES_HOST`**, **`POSTGRES_PORT`**, **`POSTGRES_USER`**, **`POSTGRES_PASSWORD`**, **`POSTGRES_DB`** (used when `DATABASE_URL` is empty). Also **`UPLOAD_DIR`**, **`MAX_UPLOAD_MB`**, **`DEBUG`**. `.env` is gitignored.


## Database migrations (Alembic)

Schema is managed with Alembic (layout similar to `Documents/sales-erp-backend`: `alembic.ini` + `migrations/`).

From the repository root (after `cp sample.env .env` or with `DATABASE_URL` in the environment):

```bash
pip install -r requirements.txt
alembic upgrade head
```

Create new revisions after model changes:

```bash
alembic revision --autogenerate -m "describe change"
alembic upgrade head
```


## Install and run the API

```bash
cp sample.env .env   # optional; defaults match docker-compose Postgres
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn data_ingestion.main:app --reload --host 0.0.0.0 --port 8000
```

- OpenAPI: http://localhost:8000/docs  
- Health: http://localhost:8000/health  
- API base: http://localhost:8000/api/v1  

## Example: upload and poll

Upload order: **stores** → **users** → **mappings**.

```bash
curl -s -X POST http://localhost:8000/api/v1/upload/stores \
  -F "file=@data/stores_master.csv"

curl -s -X POST http://localhost:8000/api/v1/upload/users \
  -F "file=@data/users_master.csv"

curl -s -X POST http://localhost:8000/api/v1/upload/mappings \
  -F "file=@data/store_user_mapping.csv"

curl -s "http://localhost:8000/api/v1/jobs/<job_id>"
```

Replace `<job_id>` with the UUID from each upload response. Mappings return `422` if stores or users tables are empty.

Uploaded CSVs are stored under `/tmp/uploads` by default (`upload_dir` / `UPLOAD_DIR` in settings). Completed uploads are deleted after processing; a hourly job also purges old files and finished job rows.
