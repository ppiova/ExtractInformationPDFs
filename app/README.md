# Azure Annual Report Extractor

Mini-proyecto reproducible para ingerir reportes anuales en PDF desde Azure Blob Storage, extraer texto y tablas con Azure AI Document Intelligence y publicar los resultados en Azure AI Search.

## Estructura

```
app/
  src/
    config.py
    blob_io.py
    extract_layout.py
    normalize_tables.py
    chunk_text.py
    build_search.py
    upsert_search.py
    utils.py
  out/
  .env.example
  requirements.txt
```

## Requisitos

* Python 3.11+
* Azure Subscription con servicios:
  * Azure Blob Storage
  * Azure AI Document Intelligence (Content Understanding v4)
  * Azure AI Search
* Variables de entorno definidas en `.env` (ver `.env.example`).

Instala dependencias:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r app/requirements.txt
```

Copia `.env.example` a `.env` y completa los valores necesarios:

```bash
cp app/.env.example .env
```

## Ejecución paso a paso

> Asegúrate de ejecutar los comandos desde la carpeta `app/`:
>
> ```bash
> cd app
> ```

1. **Extraer layout desde Azure Document Intelligence**

   ```bash
   python -m src.extract_layout
   ```

   Genera archivos `out/layout_*.json` con el layout de cada PDF.

2. **Normalizar tablas**

   ```bash
   python -m src.normalize_tables
   ```

   Produce `out/facts_FYXXXX.csv` agrupado por año fiscal.

3. **Chunkear narrativa**

   ```bash
   python -m src.chunk_text
   ```

   Produce `out/narrative.jsonl` con chunks de 1,200–1,600 tokens.

4. **Crear/actualizar índices en Azure AI Search**

   ```bash
   python -m src.build_search
   ```

5. **Subir documentos a los índices**

   ```bash
   python -m src.upsert_search
   ```

   Carga los documentos en los índices `narrative` y `tables` con lotes ≤ 500 documentos.

## Logs y métricas

Cada módulo emite logs en nivel INFO con métricas clave (#páginas, #chunks, #tablas, tiempos de ejecución). Ajusta el nivel de logging según tus necesidades.

## Pruebas

Ejecuta pruebas unitarias básicas:

```bash
pytest
```

## Notas

* Los IDs son deterministas para evitar duplicados.
* El chunking incluye solapamiento de 180 tokens para preservar contexto.
* Las tablas se convierten a formato largo para Azure Search con detección heurística del tipo de estado financiero.
