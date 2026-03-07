"""Helpers for deterministic model bundle publishing and validation."""

from __future__ import annotations

import hashlib
import os
from pathlib import Path
import tempfile


BUNDLE_REQUIRED_FILES = (
    "manifest.json",
    "global_model.txt",
    "feature_spec.json",
    "thresholds.json",
    "cost_model.json",
)


def bundle_root(*, repo_root: Path) -> Path:
    return repo_root / "models" / "bundles"


def bundle_dir(*, repo_root: Path, bundle_id: str) -> Path:
    return bundle_root(repo_root=repo_root) / bundle_id


def latest_pointer_path(*, repo_root: Path) -> Path:
    return bundle_root(repo_root=repo_root) / "LATEST"


def read_active_bundle_id(*, repo_root: Path) -> str | None:
    path = latest_pointer_path(repo_root=repo_root)
    if not path.exists():
        return None
    value = path.read_text(encoding="utf-8").strip()
    return value or None


def write_active_bundle_id_atomic(*, repo_root: Path, bundle_id: str) -> None:
    path = latest_pointer_path(repo_root=repo_root)
    _write_text_atomic(path, bundle_id.strip() + "\n")


def write_bundle_signature(*, bundle_path: Path) -> str:
    signature = compute_bundle_signature(bundle_path=bundle_path)
    _write_text_atomic(bundle_path / "signature.sha256", signature + "\n")
    return signature


def validate_bundle_signature(*, bundle_path: Path) -> tuple[bool, str | None, str]:
    signature_path = bundle_path / "signature.sha256"
    try:
        actual = compute_bundle_signature(bundle_path=bundle_path)
    except ValueError:
        return False, None, ""
    if not signature_path.exists():
        return False, None, actual
    expected = signature_path.read_text(encoding="utf-8").strip() or None
    return expected == actual, expected, actual


def compute_bundle_signature(*, bundle_path: Path) -> str:
    required_paths = [bundle_path / name for name in BUNDLE_REQUIRED_FILES]
    if not all(path.exists() for path in required_paths):
        missing = [path.name for path in required_paths if not path.exists()]
        raise ValueError(f"bundle missing required files: {', '.join(sorted(missing))}")
    if not (bundle_path / "per_coin").exists():
        raise ValueError("bundle missing per_coin directory")

    digest = hashlib.sha256()
    files = _bundle_files_for_signature(bundle_path=bundle_path)
    for file_path in files:
        relative = file_path.relative_to(bundle_path).as_posix()
        digest.update(relative.encode("utf-8"))
        digest.update(b"\0")
        digest.update(hashlib.sha256(file_path.read_bytes()).digest())
        digest.update(b"\0")
    return digest.hexdigest()


def _bundle_files_for_signature(*, bundle_path: Path) -> list[Path]:
    files: list[Path] = []
    for path in bundle_path.rglob("*"):
        if not path.is_file():
            continue
        if path.name == "signature.sha256":
            continue
        files.append(path)
    files.sort(key=lambda item: item.relative_to(bundle_path).as_posix())
    return files


def _write_text_atomic(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=path.parent,
            delete=False,
        ) as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
            tmp_path = Path(handle.name)
        os.replace(tmp_path, path)
        _fsync_directory(path.parent)
    finally:
        if tmp_path is not None and tmp_path.exists():
            tmp_path.unlink(missing_ok=True)


def _fsync_directory(directory: Path) -> None:
    fd = os.open(directory, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
