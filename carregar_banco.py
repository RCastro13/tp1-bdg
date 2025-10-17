import os
import argparse
import pandas as pd
import geopandas as gpd
from sqlalchemy import create_engine
from tqdm import tqdm
from dotenv import load_dotenv

load_dotenv()

PG_HOST = os.getenv("PG_HOST")
PG_PORT = os.getenv("PG_PORT")
PG_DB = os.getenv("PG_DB")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")

def get_postgis_engine():
    user = PG_USER
    password = PG_PASSWORD
    host = PG_HOST
    port = PG_PORT
    db = PG_DB
    return create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")

# ==========================
# Função para carregar CSVs
# ==========================
def carregar_csv(csv_path, engine, chunksize=10000):
    print(f"📄 Carregando CSV: {csv_path}")
    table_name = os.path.splitext(os.path.basename(csv_path))[0].lower()

    # 🔹 1. Detecta encoding e separador automaticamente
    encoding_ok, sep_ok = None, None
    for encoding in ["utf-8", "latin1", "iso-8859-1"]:
        try:
            pd.read_csv(csv_path, encoding=encoding, sep=None, engine='python', nrows=100, on_bad_lines='skip')
            encoding_ok = encoding
            sep_ok = None
            print(f"   ✅ Lido com sucesso usando encoding='{encoding}' (detecção automática de separador).")
            break
        except pd.errors.ParserError:
            try:
                pd.read_csv(csv_path, encoding=encoding, sep=';', engine='python', nrows=100, on_bad_lines='skip')
                encoding_ok = encoding
                sep_ok = ';'
                print(f"   ⚙️ Recarregado com separador ';' e encoding='{encoding}'.")
                break
            except Exception:
                continue
        except UnicodeDecodeError:
            continue
        except Exception as e:
            print(f"   ⚠️ Erro lendo {csv_path} com {encoding}: {e}")
            continue

    if encoding_ok is None:
        print(f"❌ Não foi possível ler {csv_path}.")
        return False

    # 🔹 2. Conta linhas para mostrar progresso
    try:
        total_linhas = sum(1 for _ in open(csv_path, encoding=encoding_ok, errors="ignore")) - 1
    except Exception:
        total_linhas = None

    print(f"   📦 Iniciando carga em batches de {chunksize} linhas...")

    # 🔹 3. Lê em batches (chunks) e envia para o banco
    try:
        first_chunk = True
        with tqdm(total=total_linhas, unit="linhas", desc=f"→ {table_name}", ncols=100) as pbar:
            for chunk in pd.read_csv(
                csv_path,
                encoding=encoding_ok,
                sep=sep_ok,
                engine="python",
                chunksize=chunksize,
                on_bad_lines="skip"
            ):
                if chunk.empty or len(chunk.columns) == 0:
                    continue

                chunk.to_sql(
                    table_name,
                    engine,
                    if_exists="replace" if first_chunk else "append",
                    index=False,
                    method="multi"
                )

                first_chunk = False
                pbar.update(len(chunk))

        print(f"✅ Tabela '{table_name}' criada no banco com sucesso!\n")
        return True

    except Exception as e:
        print(f"❌ Erro ao salvar '{table_name}' no banco: {e}\n")
        return False


# ==========================
# Função para carregar Shapefiles
# ==========================
def carregar_shapefile(shp_path, engine):
    print(f"🗺️ Carregando Shapefile: {shp_path}")
    table_name = os.path.splitext(os.path.basename(shp_path))[0].lower()

    try:
        gdf = gpd.read_file(shp_path)
        gdf.to_postgis(table_name, engine, if_exists='replace', index=False)
        print(f"✅ Tabela '{table_name}' criada no banco (PostGIS).")
        return True
    except Exception as e:
        print(f"❌ Erro ao processar {shp_path}: {e}")
        return False


# ==========================
# Processadores de pastas
# ==========================
def processar_pasta_csv(csv_dir, engine):
    sucesso, falha = 0, 0
    for root, _, files in os.walk(csv_dir):
        for file in files:
            if file.lower().endswith(".csv"):
                ok = carregar_csv(os.path.join(root, file), engine)
                sucesso += int(ok)
                falha += int(not ok)
    print(f"\n📊 Resumo CSVs: {sucesso} sucesso(s), {falha} falha(s)\n")

def processar_pasta_shp(shp_dir, engine):
    sucesso, falha = 0, 0
    for root, _, files in os.walk(shp_dir):
        for file in files:
            if file.lower().endswith(".shp"):
                ok = carregar_shapefile(os.path.join(root, file), engine)
                sucesso += int(ok)
                falha += int(not ok)
    print(f"\n📊 Resumo Shapefiles: {sucesso} sucesso(s), {falha} falha(s)\n")


# ==========================
# Função principal
# ==========================
def main():
    parser = argparse.ArgumentParser(description="Carrega CSVs e Shapefiles em um banco PostGIS.")
    parser.add_argument("--csv_dir", type=str, help="Caminho da pasta com arquivos CSV.")
    parser.add_argument("--shp_dir", type=str, help="Caminho da pasta com shapefiles.")
    args = parser.parse_args()

    engine = get_postgis_engine()

    if args.csv_dir:
        print("\n=== Iniciando upload de CSVs ===")
        processar_pasta_csv(args.csv_dir, engine)

    if args.shp_dir:
        print("\n=== Iniciando upload de Shapefiles ===")
        processar_pasta_shp(args.shp_dir, engine)

    if not args.csv_dir and not args.shp_dir:
        print("⚠️ Nenhum diretório informado. Use --csv_dir e/ou --shp_dir.")


if __name__ == "__main__":
    main()
