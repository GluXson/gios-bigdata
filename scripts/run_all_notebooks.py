from pathlib import Path
import sys

import nbformat
from nbclient import NotebookClient


def main():
    root = Path(__file__).resolve().parent
    if root.name == "scripts":
        root = root.parent
    folder = root / "notebooks"
    if not folder.is_dir():
        print(f"BŁĄD: nie znaleziono katalogu {folder}", flush=True)
        return 1

    notebooks = []
    for path in folder.iterdir():
        if path.is_file() and path.suffix == ".ipynb":
            try:
                number = int(path.name[:2])
            except ValueError:
                continue
            notebooks.append((number, path))
    notebooks.sort(key=lambda item: (item[0], item[1].name))

    print("Lista notebooków do wykonania:", flush=True)
    for number, path in notebooks:
        print(f"{number:02d}: {path.name}", flush=True)
    if not notebooks:
        print("Brak notebooków z numerem w pierwszych dwóch znakach nazwy.", flush=True)
        return 0

    for _, path in notebooks:
        print(f"\nURUCHAMIAM: {path.name}", flush=True)
        notebook = None
        try:
            notebook = nbformat.read(path, as_version=4)
            client = NotebookClient(
                notebook,
                timeout=None,
                kernel_name="python3",
                allow_errors=False,
                resources={"metadata": {"path": str(folder)}},
            )
            client.execute()
            nbformat.write(notebook, path)
        except Exception as exc:
            print(f"\nBŁĄD W NOTEBOOKU: {path.name}\n{exc}", flush=True)
            if notebook is not None:
                try:
                    nbformat.write(notebook, path)
                except Exception as save_error:
                    print(f"Nie udało się zapisać wyników: {save_error}", flush=True)
            print("Zatrzymano. Kolejne notebooki nie zostaną uruchomione.", flush=True)
            return 1
        except KeyboardInterrupt:
            print(f"\nPrzerwano: {path.name}. Zatrzymano wykonywanie.", flush=True)
            return 130
        print(f"OK: {path.name}", flush=True)

    print("\nWszystkie notebooki wykonane.", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
