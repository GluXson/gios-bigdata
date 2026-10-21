import hashlib
import json
import logging
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = ROOT / "config" / "sources.json"
RAW_DIR = ROOT / "data" / "raw"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)


def create_session():
    retries = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )

    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retries))
    session.mount("http://", HTTPAdapter(max_retries=retries))

    return session


def sha256_file(path):
    digest = hashlib.sha256()

    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)

    return digest.hexdigest()


def source_folder(source):
    if "dataset" in source:
        return source["dataset"]

    return str(source["year"])


def load_sources():
    if not CONFIG_PATH.is_file():
        raise FileNotFoundError(
            f"Nie znaleziono konfiguracji: {CONFIG_PATH}"
        )

    config = json.loads(
        CONFIG_PATH.read_text(encoding="utf-8-sig")
    )

    sources = config.get("sources", [])

    if not isinstance(sources, list) or not sources:
        raise ValueError(
            "Pole 'sources' musi być niepustą listą "
            "w config/sources.json."
        )

    destinations = set()

    for source in sources:
        validate_source(source)

        destination = (
            source_folder(source),
            source["filename"].casefold(),
        )

        if destination in destinations:
            raise ValueError(
                "Powtórzony plik docelowy: "
                f"{destination[0]}/{source['filename']}"
            )

        destinations.add(destination)

    return sources


def validate_source(source):
    folder = source_folder(source)
    url = source["url"].strip()
    filename = source["filename"]

    if "dataset" in source:
        if (
            not folder
            or folder.startswith(("/", "\\"))
            or ".." in folder.replace("\\", "/").split("/")
        ):
            raise ValueError(f"Nieprawidłowy dataset: {folder!r}")
    elif not folder.isdigit() or len(folder) != 4:
        raise ValueError(f"Nieprawidłowy rok: {folder}")

    parsed_url = urlparse(url)

    if (
        parsed_url.scheme not in {"http", "https"}
        or not parsed_url.netloc
    ):
        raise ValueError(
            f"Uzupełnij bezpośredni adres pliku dla {folder} "
            "w config/sources.json."
        )

        if (
            not isinstance(filename, str)
            or not filename
            or filename in {".", ".."}
            or any(char in filename for char in '<>:"/\\|?*')
            or filename.endswith((" ", "."))
        ):
            raise ValueError(
                f"Nieprawidłowa nazwa pliku: {filename!r}"
            )


def verify_existing_file(target, metadata_path, source_url):
    if not metadata_path.is_file():
        raise RuntimeError(
            f"Plik istnieje bez metadanych: {target}. "
            "Sprawdź go ręcznie przed ponownym importem."
        )

    metadata = json.loads(
        metadata_path.read_text(encoding="utf-8")
    )

    if metadata.get("source_url") != source_url:
        raise RuntimeError(
            f"Inny adres źródłowy dla istniejącego pliku: {target}"
        )

    if target.stat().st_size != metadata.get("size_bytes"):
        raise RuntimeError(
            f"Niezgodny rozmiar pliku: {target}"
        )

    if sha256_file(target) != metadata.get("sha256"):
        raise RuntimeError(
            f"Niezgodna suma kontrolna: {target}"
        )

    logger.info(
        "Plik istnieje i jest poprawny — pomijam: %s",
        target.name,
    )

    return metadata


def import_source(session, source):
    validate_source(source)

    folder = source_folder(source)
    source_url = source["url"].strip()
    filename = source["filename"]

    target_dir = RAW_DIR / folder
    target_dir.mkdir(parents=True, exist_ok=True)

    target = target_dir / filename
    temporary = target.with_name(target.name + ".part")

    metadata_path = target.with_name(
        target.name + ".metadata.json"
    )
    metadata_temporary = metadata_path.with_name(
        metadata_path.name + ".part"
    )

    if target.exists():
        return verify_existing_file(
            target,
            metadata_path,
            source_url,
        )

    logger.info("Pobieranie: %s", source_url)

    try:
        with session.get(
            source_url,
            stream=True,
            timeout=(15, 180),
        ) as response:
            response.raise_for_status()

            content_type = response.headers.get(
                "Content-Type", ""
            )
            resolved_url = response.url

            if "text/html" in content_type.lower():
                raise ValueError(
                    "Serwer zwrócił stronę HTML zamiast pliku. "
                    "Podaj bezpośredni adres pobrania."
                )

            with temporary.open("wb") as file:
                for chunk in response.iter_content(
                    chunk_size=1024 * 1024
                ):
                    if chunk:
                        file.write(chunk)

        size_bytes = temporary.stat().st_size

        if size_bytes == 0:
            raise ValueError("Pobrany plik jest pusty.")

        if target.suffix.lower() == ".zip":
            if not zipfile.is_zipfile(temporary):
                raise ValueError(
                    "Pobrany plik nie jest archiwum ZIP."
                )

        metadata = {
            "provider": source.get("provider", "GIOŚ"),
            "layer": "raw",
            "dataset": folder,
            "year": source.get("year"),
            "source_url": source_url,
            "resolved_url": resolved_url,
            "filename": filename,
            "downloaded_at_utc": datetime.now(
                timezone.utc
            ).isoformat(),
            "size_bytes": size_bytes,
            "sha256": sha256_file(temporary),
            "content_type": content_type,
        }

        metadata_temporary.write_text(
            json.dumps(
                metadata,
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )

        temporary.replace(target)
        metadata_temporary.replace(metadata_path)

        logger.info(
            "Zapisano: %s | %.2f MB",
            target,
            size_bytes / 1_000_000,
        )

        return metadata

    finally:
        temporary.unlink(missing_ok=True)
        metadata_temporary.unlink(missing_ok=True)


def main():
    sources = load_sources()
    results = []

    with create_session() as session:
        for source in sources:
            results.append(import_source(session, source))

    logger.info("Import do RAW zakończony.")

    return results


if __name__ == "__main__":
    main()
    