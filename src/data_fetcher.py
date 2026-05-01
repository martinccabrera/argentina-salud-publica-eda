"""
data_fetcher.py
---------------
Descarga datasets de salud pública del Ministerio de Salud de Argentina
usando links directos a CSVs oficiales (más robusto que la API de CKAN).

Datasets:
  1. Defunciones por provincia, sexo, causa y edad (2020-2024) — DEIS
  2. Serie histórica de defunciones por jurisdicción (1914-2023)

Uso:
  python data_fetcher.py

Los archivos se guardan en ../data/raw/
"""

import requests
import pandas as pd
import urllib3
from pathlib import Path

# Silenciar warnings de SSL (portal del gobierno con cert no verificable)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ── Configuración ──────────────────────────────────────────────────────────────

RAW_DATA_DIR = Path(__file__).parent.parent / "data" / "raw"

# Links directos a CSVs oficiales del Ministerio de Salud (DEIS)
DATASETS = {
    "defunciones_2024": {
        "url": "https://www.argentina.gob.ar/sites/default/files/2018/12/defunciones_2024.csv",
        "desc": "Defunciones por provincia, sexo, causa y edad — 2024"
    },
    "defunciones_2023": {
        "url": "https://www.argentina.gob.ar/sites/default/files/2018/12/defunciones_2023.csv",
        "desc": "Defunciones por provincia, sexo, causa y edad — 2023"
    },
    "defunciones_2022": {
        "url": "https://www.argentina.gob.ar/sites/default/files/2018/12/defunciones_2022.csv",
        "desc": "Defunciones por provincia, sexo, causa y edad — 2022"
    },
    "defunciones_2021": {
        "url": "https://www.argentina.gob.ar/sites/default/files/2018/12/defunciones_2021.csv",
        "desc": "Defunciones por provincia, sexo, causa y edad — 2021"
    },
    "defunciones_2020": {
        "url": "https://www.argentina.gob.ar/sites/default/files/2018/12/defunciones_2020.csv",
        "desc": "Defunciones por provincia, sexo, causa y edad — 2020"
    },
    "serie_historica": {
        "url": "https://datos.salud.gob.ar/dataset/76ce7d19-921d-4d17-bb20-73c1fe55f07e/resource/a4d3c1c7-63c3-4ad6-91e7-ffef6198c8c0/download/serie-historica-ocurrida-argentina-jurisdiccion-1914-2023.csv",
        "desc": "Serie histórica de defunciones por jurisdicción 1914-2023"
    },
}


# ── Funciones ──────────────────────────────────────────────────────────────────

def download_file(name: str, url: str, desc: str, dest_dir: Path) -> Path | None:
    """Descarga un archivo CSV y lo guarda en dest_dir."""
    filename = f"{name}.csv"
    dest_path = dest_dir / filename

    if dest_path.exists():
        print(f"  → Ya existe, se omite: {filename}")
        return dest_path

    print(f"  → Descargando: {desc} ...", end=" ", flush=True)
    try:
        r = requests.get(url, timeout=60, verify=False)
        r.raise_for_status()
        dest_path.write_bytes(r.content)
        size_kb = dest_path.stat().st_size / 1024
        print(f"OK ({size_kb:.1f} KB)")
        return dest_path
    except Exception as e:
        print(f"ERROR: {e}")
        return None


def fetch_all() -> dict[str, Path]:
    """Descarga todos los datasets y devuelve {nombre: path}."""
    RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
    downloaded = {}

    print(f"\n📥 Descargando {len(DATASETS)} datasets...\n")
    for name, info in DATASETS.items():
        path = download_file(name, info["url"], info["desc"], RAW_DATA_DIR)
        if path:
            downloaded[name] = path

    return downloaded


def preview(downloaded: dict[str, Path]) -> None:
    """Muestra un resumen de cada dataset descargado."""
    print("\n" + "=" * 60)
    print("PREVIEW DE DATASETS")
    print("=" * 60)

    for name, path in downloaded.items():
        print(f"\n📊 {name}")
        try:
            for enc in ("utf-8", "latin-1", "iso-8859-1"):
                try:
                    df = pd.read_csv(path, encoding=enc, low_memory=False)
                    break
                except UnicodeDecodeError:
                    continue

            print(f"   Filas × Columnas : {df.shape[0]:,} × {df.shape[1]}")
            print(f"   Columnas         : {list(df.columns)}")
            nulos = df.isnull().sum()
            nulos = nulos[nulos > 0].to_dict()
            if nulos:
                print(f"   Columnas con nulos: {nulos}")
            print("   Primeras 3 filas:")
            print(df.head(3).to_string(index=False))
        except Exception as e:
            print(f"   ❌ No se pudo leer: {e}")


# ── Entry point ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("🏥 Fetcher de datos de salud pública — Argentina")
    print(f"   Destino: {RAW_DATA_DIR.resolve()}")

    downloaded = fetch_all()

    print(f"\n✅ Descarga completa: {len(downloaded)} archivo(s) en {RAW_DATA_DIR}")

    preview(downloaded)
