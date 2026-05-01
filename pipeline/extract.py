import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from typing import Optional
from pipeline.config import LOCAL_DATA_PATH, PARQUET_DIR


def _out_path(p: Path) -> Path:
    out = (PARQUET_DIR / p.relative_to(LOCAL_DATA_PATH)).with_suffix(".parquet")
    out.parent.mkdir(parents=True, exist_ok=True)
    return out


def _convert(csv: Path, chunksize: int = 100_000) -> Optional[Path]:
    try:
        out = _out_path(csv)
        writer = None
        for chunk in pd.read_csv(csv, chunksize=chunksize):
            table = pa.Table.from_pandas(chunk, preserve_index=False)
            if writer is None:
                writer = pq.ParquetWriter(out, table.schema, compression="snappy")
            writer.write_table(table)
        if writer:
            writer.close()
        print(f"✔ {csv.name} → {out}")
        return out
    except Exception as e:
        print(f"✘ {csv.name}: {e}")


def convert_all(max_workers: int = 4) -> list[Path]:
    PARQUET_DIR.mkdir(parents=True, exist_ok=True)
    csvs = [p for p in LOCAL_DATA_PATH.rglob("*.csv")
            if PARQUET_DIR not in p.parents]
    if not csvs:
        print("No CSV files found.")
        return []
    print(f"Found {len(csvs)} CSV(s). Converting...\n")
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        results = ex.map(_convert, csvs)
    return [r for r in results if r]


if __name__ == "__main__":
    convert_all()