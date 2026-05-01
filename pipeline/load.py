import re
import pyarrow.parquet as pq
from sqlalchemy import create_engine, text
from pathlib import Path
from pipeline.config import connection_string, PARQUET_DIR

_engine = create_engine(connection_string, pool_pre_ping=True)

_VALID_SCHEMA = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


def _ensure_schema(schema: str) -> None:
    if not _VALID_SCHEMA.match(schema):
        raise ValueError(f"Invalid schema name: {schema!r}")
    with _engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))


def load(parquet_path: Path, schema: str = "raw") -> None:
    table = parquet_path.stem.lower().replace(" ", "_")
    file = pq.ParquetFile(parquet_path)
    total = 0
    for i, batch in enumerate(file.iter_batches(batch_size=5_000)):
        df = batch.to_pandas()
        df.to_sql(table, _engine, schema=schema,
                  if_exists="replace" if i == 0 else "append",
                  index=False)
        total += len(df)
    print(f"✔ {table} → {schema}.{table} ({total:,} rows)")


def load_all(schema: str = "raw") -> None:
    parquets = list(PARQUET_DIR.rglob("*.parquet"))
    if not parquets:
        print("No parquet files found.")
        return
    _ensure_schema(schema)
    print(f"Loading {len(parquets)} file(s) into '{schema}'...\n")
    for p in parquets:
        load(p, schema)


if __name__ == "__main__":
    load_all()