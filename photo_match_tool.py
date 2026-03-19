#!/usr/bin/env python3
"""Match likely duplicated Apple Photos assets across two libraries or exports."""

from __future__ import annotations

import argparse
import csv
import hashlib
import html
import json
import os
import sqlite3
import shutil
import subprocess
import sys
import tempfile
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable


MEDIA_EXTENSIONS = {
    ".3gp",
    ".avi",
    ".bmp",
    ".dng",
    ".gif",
    ".heic",
    ".jpeg",
    ".jpg",
    ".m4v",
    ".mov",
    ".mp4",
    ".mts",
    ".orf",
    ".png",
    ".raf",
    ".rw2",
    ".tif",
    ".tiff",
    ".webp",
}

CHUNK_SIZE = 1024 * 1024
VERSION = "0.1.0"
HASH_PROGRESS_INTERVAL = 250


@dataclass(frozen=True)
class FileRecord:
    root: str
    relative_path: str
    filename: str
    extension: str
    size_bytes: int
    sha256: str

    @property
    def absolute_path(self) -> Path:
        return Path(self.root) / self.relative_path


@dataclass(frozen=True)
class ExportFileCandidate:
    root: str
    relative_path: str
    filename: str
    extension: str
    size_bytes: int

    @property
    def absolute_path(self) -> Path:
        return Path(self.root) / self.relative_path


APPLE_EPOCH_OFFSET = 978307200


@dataclass(frozen=True)
class LibraryRecord:
    local_uuid: str
    filename: str
    original_filename: str
    created_at_utc: str
    added_at_utc: str
    duration_seconds: float
    width: int
    height: int
    original_filesize: int
    original_stablehash: str
    adjusted_stablehash: str
    imported_by_bundle_identifier: str
    kind: int
    kind_subtype: int
    favorite: int


def iter_media_files(root: Path) -> Iterable[Path]:
    for path in sorted(root.rglob("*")):
        if not path.is_file():
            continue
        if path.name.startswith("."):
            continue
        if path.suffix.lower() not in MEDIA_EXTENSIONS:
            continue
        yield path


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(CHUNK_SIZE):
            digest.update(chunk)
    return digest.hexdigest()


def scan_export_candidates(root: Path) -> list[ExportFileCandidate]:
    candidates: list[ExportFileCandidate] = []
    for path in iter_media_files(root):
        relative_path = path.relative_to(root).as_posix()
        stat = path.stat()
        candidates.append(
            ExportFileCandidate(
                root=str(root.resolve()),
                relative_path=relative_path,
                filename=path.name,
                extension=path.suffix.lower(),
                size_bytes=stat.st_size,
            )
        )
    return candidates


def build_manifest_from_candidates(
    candidates: list[ExportFileCandidate], label: str | None = None
) -> list[FileRecord]:
    records: list[FileRecord] = []
    total = len(candidates)
    for index, candidate in enumerate(candidates, start=1):
        if label and (index == 1 or index % HASH_PROGRESS_INTERVAL == 0 or index == total):
            print(
                f"[{label}] hashing {index}/{total}: {candidate.relative_path}",
                file=sys.stderr,
            )
        records.append(
            FileRecord(
                root=candidate.root,
                relative_path=candidate.relative_path,
                filename=candidate.filename,
                extension=candidate.extension,
                size_bytes=candidate.size_bytes,
                sha256=sha256_file(candidate.absolute_path),
            )
        )
    return records


def build_manifest(root: Path, label: str | None = None) -> list[FileRecord]:
    return build_manifest_from_candidates(scan_export_candidates(root), label=label)


def candidate_keys(candidates: list[ExportFileCandidate]) -> set[tuple[int, str]]:
    return {(candidate.size_bytes, candidate.extension) for candidate in candidates}


def filter_candidates_for_overlap(
    candidates: list[ExportFileCandidate], allowed_keys: set[tuple[int, str]]
) -> list[ExportFileCandidate]:
    return [
        candidate
        for candidate in candidates
        if (candidate.size_bytes, candidate.extension) in allowed_keys
    ]


def write_manifest_csv(records: list[FileRecord], output_path: Path) -> None:
    with output_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "root",
                "relative_path",
                "absolute_path",
                "filename",
                "extension",
                "size_bytes",
                "sha256",
            ],
        )
        writer.writeheader()
        for record in records:
            writer.writerow(
                {
                    "root": record.root,
                    "relative_path": record.relative_path,
                    "absolute_path": str(record.absolute_path),
                    "filename": record.filename,
                    "extension": record.extension,
                    "size_bytes": record.size_bytes,
                    "sha256": record.sha256,
                }
            )


def load_manifest_csv(path: Path) -> list[FileRecord]:
    records: list[FileRecord] = []
    with path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            records.append(
                FileRecord(
                    root=row["root"],
                    relative_path=row["relative_path"],
                    filename=row["filename"],
                    extension=row["extension"],
                    size_bytes=int(row["size_bytes"]),
                    sha256=row["sha256"],
                )
            )
    return records


def apple_time_to_utc(value: float | int | None) -> str:
    if value is None:
        return ""
    unix_ts = float(value) + APPLE_EPOCH_OFFSET
    return datetime.fromtimestamp(unix_ts, tz=timezone.utc).isoformat()


def normalize_created_second(value: str) -> str:
    if not value:
        return ""
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat()
    except ValueError:
        return value


def default_photos_library_path() -> Path:
    return Path.home() / "Pictures" / "Photos Library.photoslibrary" / "database" / "Photos.sqlite"


def connect_readonly_sqlite(path: Path) -> sqlite3.Connection:
    uri = f"file:{path.as_posix()}?mode=ro&immutable=1"
    connection = sqlite3.connect(uri, uri=True)
    connection.row_factory = sqlite3.Row
    return connection


def ensure_path_exists(path: Path, description: str) -> None:
    if not path.exists():
        raise FileNotFoundError(f"{description} not found: {path}")


def build_library_manifest(photos_db_path: Path) -> list[LibraryRecord]:
    query = """
    SELECT
      a.ZUUID AS local_uuid,
      a.ZFILENAME AS filename,
      COALESCE(aa.ZORIGINALFILENAME, a.ZFILENAME) AS original_filename,
      a.ZDATECREATED AS created_at_raw,
      a.ZADDEDDATE AS added_at_raw,
      COALESCE(a.ZDURATION, 0.0) AS duration_seconds,
      COALESCE(a.ZWIDTH, 0) AS width,
      COALESCE(a.ZHEIGHT, 0) AS height,
      COALESCE(aa.ZORIGINALFILESIZE, 0) AS original_filesize,
      COALESCE(aa.ZORIGINALSTABLEHASH, '') AS original_stablehash,
      COALESCE(aa.ZADJUSTEDSTABLEHASH, '') AS adjusted_stablehash,
      COALESCE(aa.ZIMPORTEDBYBUNDLEIDENTIFIER, '') AS imported_by_bundle_identifier,
      COALESCE(a.ZKIND, 0) AS kind,
      COALESCE(a.ZKINDSUBTYPE, 0) AS kind_subtype,
      COALESCE(a.ZFAVORITE, 0) AS favorite
    FROM ZASSET a
    LEFT JOIN ZADDITIONALASSETATTRIBUTES aa
      ON aa.Z_PK = a.ZADDITIONALATTRIBUTES
    WHERE a.ZTRASHEDSTATE = 0
      AND a.ZHIDDEN = 0
      AND a.ZVISIBILITYSTATE = 0
      AND a.ZUUID IS NOT NULL
    ORDER BY a.ZDATECREATED, a.ZADDEDDATE, a.Z_PK
    """
    records: list[LibraryRecord] = []
    with connect_readonly_sqlite(photos_db_path) as connection:
        for row in connection.execute(query):
            records.append(
                LibraryRecord(
                    local_uuid=row["local_uuid"],
                    filename=row["filename"] or "",
                    original_filename=row["original_filename"] or "",
                    created_at_utc=apple_time_to_utc(row["created_at_raw"]),
                    added_at_utc=apple_time_to_utc(row["added_at_raw"]),
                    duration_seconds=float(row["duration_seconds"] or 0.0),
                    width=int(row["width"] or 0),
                    height=int(row["height"] or 0),
                    original_filesize=int(row["original_filesize"] or 0),
                    original_stablehash=row["original_stablehash"] or "",
                    adjusted_stablehash=row["adjusted_stablehash"] or "",
                    imported_by_bundle_identifier=row["imported_by_bundle_identifier"] or "",
                    kind=int(row["kind"] or 0),
                    kind_subtype=int(row["kind_subtype"] or 0),
                    favorite=int(row["favorite"] or 0),
                )
            )
    return records


def write_library_manifest_csv(records: list[LibraryRecord], output_path: Path) -> None:
    with output_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "local_uuid",
                "filename",
                "original_filename",
                "created_at_utc",
                "added_at_utc",
                "duration_seconds",
                "width",
                "height",
                "original_filesize",
                "original_stablehash",
                "adjusted_stablehash",
                "imported_by_bundle_identifier",
                "kind",
                "kind_subtype",
                "favorite",
            ],
        )
        writer.writeheader()
        for record in records:
            writer.writerow(record.__dict__)


def load_library_manifest_csv(path: Path) -> list[LibraryRecord]:
    records: list[LibraryRecord] = []
    with path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            records.append(
                LibraryRecord(
                    local_uuid=row["local_uuid"],
                    filename=row["filename"],
                    original_filename=row["original_filename"],
                    created_at_utc=row["created_at_utc"],
                    added_at_utc=row["added_at_utc"],
                    duration_seconds=float(row["duration_seconds"] or 0.0),
                    width=int(row["width"] or 0),
                    height=int(row["height"] or 0),
                    original_filesize=int(row["original_filesize"] or 0),
                    original_stablehash=row["original_stablehash"],
                    adjusted_stablehash=row["adjusted_stablehash"],
                    imported_by_bundle_identifier=row["imported_by_bundle_identifier"],
                    kind=int(row["kind"] or 0),
                    kind_subtype=int(row["kind_subtype"] or 0),
                    favorite=int(row["favorite"] or 0),
                )
            )
    return records


def metadata_fallback_key(record: LibraryRecord) -> str:
    created = normalize_created_second(record.created_at_utc)
    duration_bucket = round(record.duration_seconds, 3)
    return "|".join(
        [
            record.original_filename.lower(),
            created,
            str(record.original_filesize),
            str(record.width),
            str(record.height),
            f"{duration_bucket:.3f}",
            str(record.kind),
            str(record.kind_subtype),
        ]
    )


def compare_library_manifests(
    source_records: list[LibraryRecord], target_records: list[LibraryRecord]
) -> tuple[list[dict[str, str | int | float]], dict[str, int]]:
    source_by_hash: dict[str, list[LibraryRecord]] = defaultdict(list)
    source_by_fallback: dict[str, list[LibraryRecord]] = defaultdict(list)

    for record in source_records:
        if record.original_stablehash:
            source_by_hash[record.original_stablehash].append(record)
        else:
            source_by_fallback[metadata_fallback_key(record)].append(record)

    rows: list[dict[str, str | int | float]] = []
    matched_target_ids = set()
    matched_hashes = set()
    matched_fallback = set()

    for target in target_records:
        matched = False
        if target.original_stablehash and source_by_hash.get(target.original_stablehash):
            for source in source_by_hash[target.original_stablehash]:
                rows.append(
                    {
                        "match_type": "original_stablehash",
                        "confidence": "high",
                        "source_local_uuid": source.local_uuid,
                        "target_local_uuid": target.local_uuid,
                        "source_original_filename": source.original_filename,
                        "target_original_filename": target.original_filename,
                        "source_created_at_utc": source.created_at_utc,
                        "target_created_at_utc": target.created_at_utc,
                        "source_added_at_utc": source.added_at_utc,
                        "target_added_at_utc": target.added_at_utc,
                        "source_original_filesize": source.original_filesize,
                        "target_original_filesize": target.original_filesize,
                        "source_width": source.width,
                        "source_height": source.height,
                        "target_width": target.width,
                        "target_height": target.height,
                        "source_duration_seconds": source.duration_seconds,
                        "target_duration_seconds": target.duration_seconds,
                        "original_stablehash": target.original_stablehash,
                        "target_imported_by_bundle_identifier": target.imported_by_bundle_identifier,
                    }
                )
            matched = True
            matched_hashes.add(target.original_stablehash)
        else:
            fallback_key = metadata_fallback_key(target)
            if source_by_fallback.get(fallback_key):
                for source in source_by_fallback[fallback_key]:
                    rows.append(
                        {
                            "match_type": "metadata_fallback",
                            "confidence": "medium",
                            "source_local_uuid": source.local_uuid,
                            "target_local_uuid": target.local_uuid,
                            "source_original_filename": source.original_filename,
                            "target_original_filename": target.original_filename,
                            "source_created_at_utc": source.created_at_utc,
                            "target_created_at_utc": target.created_at_utc,
                            "source_added_at_utc": source.added_at_utc,
                            "target_added_at_utc": target.added_at_utc,
                            "source_original_filesize": source.original_filesize,
                            "target_original_filesize": target.original_filesize,
                            "source_width": source.width,
                            "source_height": source.height,
                            "target_width": target.width,
                            "target_height": target.height,
                            "source_duration_seconds": source.duration_seconds,
                            "target_duration_seconds": target.duration_seconds,
                            "original_stablehash": "",
                            "target_imported_by_bundle_identifier": target.imported_by_bundle_identifier,
                        }
                    )
                matched = True
                matched_fallback.add(fallback_key)
        if matched:
            matched_target_ids.add(target.local_uuid)

    summary = {
        "source_total_assets": len(source_records),
        "target_total_assets": len(target_records),
        "matched_target_assets": len(matched_target_ids),
        "matched_by_original_stablehash": len(matched_hashes),
        "matched_by_metadata_fallback": len(matched_fallback),
    }
    return rows, summary


def write_library_matches_csv(
    rows: list[dict[str, str | int | float]], output_path: Path
) -> None:
    fieldnames = [
        "match_type",
        "confidence",
        "source_local_uuid",
        "target_local_uuid",
        "source_original_filename",
        "target_original_filename",
        "source_created_at_utc",
        "target_created_at_utc",
        "source_added_at_utc",
        "target_added_at_utc",
        "source_original_filesize",
        "target_original_filesize",
        "source_width",
        "source_height",
        "target_width",
        "target_height",
        "source_duration_seconds",
        "target_duration_seconds",
        "original_stablehash",
        "target_imported_by_bundle_identifier",
    ]
    with output_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def write_target_uuid_list(rows: list[dict[str, str | int | float]], output_path: Path) -> None:
    seen: set[str] = set()
    uuids: list[str] = []
    for row in rows:
        uuid = str(row["target_local_uuid"])
        if uuid in seen:
            continue
        seen.add(uuid)
        uuids.append(uuid)
    output_path.write_text("\n".join(uuids) + ("\n" if uuids else ""), encoding="utf-8")


def write_library_review_html(
    rows: list[dict[str, str | int | float]], summary: dict[str, int], output_path: Path
) -> None:
    unique_rows: dict[str, dict[str, str | int | float]] = {}
    for row in rows:
        unique_rows.setdefault(str(row["target_local_uuid"]), row)

    cards = []
    for row in unique_rows.values():
        cards.append(
            f"""
            <article class="card">
              <p class="confidence">{html.escape(str(row["confidence"]))} confidence via {html.escape(str(row["match_type"]))}</p>
              <p><strong>{html.escape(str(row["target_original_filename"]))}</strong></p>
              <p>Target UUID: <span class="mono">{html.escape(str(row["target_local_uuid"]))}</span></p>
              <p>Created: {html.escape(str(row["target_created_at_utc"]))}</p>
              <p>Added: {html.escape(str(row["target_added_at_utc"]))}</p>
              <p>Size: {html.escape(str(row["target_original_filesize"]))} bytes</p>
              <p>Dimensions: {html.escape(str(row["target_width"]))} x {html.escape(str(row["target_height"]))}</p>
              <p>Duration: {html.escape(str(row["target_duration_seconds"]))} sec</p>
              <p>Imported by: <span class="mono">{html.escape(str(row["target_imported_by_bundle_identifier"]))}</span></p>
              <p>Matched source UUID: <span class="mono">{html.escape(str(row["source_local_uuid"]))}</span></p>
            </article>
            """
        )

    page = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Library Match Review</title>
  <style>
    :root {{
      --bg: #f4f1eb;
      --panel: #fffdf9;
      --ink: #1e1b18;
      --muted: #675f56;
      --line: #dbd1c6;
      --accent: #0d5b66;
    }}
    body {{
      margin: 0;
      font-family: Georgia, "Iowan Old Style", serif;
      background: var(--bg);
      color: var(--ink);
    }}
    header {{
      padding: 24px;
      position: sticky;
      top: 0;
      background: rgba(255, 253, 249, 0.93);
      border-bottom: 1px solid var(--line);
      backdrop-filter: blur(8px);
    }}
    h1 {{ margin: 0 0 8px; }}
    main {{
      display: grid;
      grid-template-columns: repeat(auto-fill, minmax(320px, 1fr));
      gap: 16px;
      padding: 24px;
    }}
    .card {{
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 18px;
      padding: 16px;
      box-shadow: 0 8px 24px rgba(40, 29, 18, 0.06);
    }}
    p {{ margin: 0 0 8px; color: var(--muted); }}
    strong {{ color: var(--accent); }}
    .mono {{ font-family: Menlo, Monaco, monospace; word-break: break-word; }}
    .confidence {{ text-transform: uppercase; letter-spacing: 0.06em; font-size: 12px; }}
  </style>
</head>
<body>
  <header>
    <h1>Target Library Review</h1>
    <p>{summary["matched_target_assets"]} target assets matched. {summary["matched_by_original_stablehash"]} via stable hash, {summary["matched_by_metadata_fallback"]} via metadata fallback.</p>
  </header>
  <main>
    {''.join(cards)}
  </main>
</body>
</html>
"""
    output_path.write_text(page, encoding="utf-8")


def escape_applescript_string(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"')


def create_photos_review_album(uuids: list[str], album_name: str) -> str:
    match_blocks = "\n".join(
        [
            f'''  set matchedItems to (every media item whose id starts with "{escape_applescript_string(uuid)}")
  if (count matchedItems) > 0 then
    add matchedItems to targetAlbum
    set addedCount to addedCount + (count matchedItems)
  end if'''
            for uuid in uuids
        ]
    )
    script = f"""
tell application "Photos"
  activate
  if exists album "{escape_applescript_string(album_name)}" then
    set targetAlbum to album "{escape_applescript_string(album_name)}"
  else
    set targetAlbum to make new album named "{escape_applescript_string(album_name)}"
  end if

  set addedCount to 0
{match_blocks}

  return "album=" & "{escape_applescript_string(album_name)}" & ",items_added=" & (addedCount as string)
end tell
"""
    with tempfile.NamedTemporaryFile("w", suffix=".applescript", delete=False, encoding="utf-8") as handle:
        handle.write(script)
        script_path = handle.name
    try:
        completed = subprocess.run(
            ["osascript", script_path],
            text=True,
            capture_output=True,
            check=True,
        )
    finally:
        Path(script_path).unlink(missing_ok=True)
    return completed.stdout.strip()


def match_records(
    source_records: list[FileRecord], target_records: list[FileRecord]
) -> tuple[list[dict[str, str | int]], dict[str, int]]:
    source_by_hash: dict[str, list[FileRecord]] = defaultdict(list)
    for record in source_records:
        source_by_hash[record.sha256].append(record)

    rows: list[dict[str, str | int]] = []
    matched_hashes = set()
    matched_target_paths = set()
    for target in target_records:
        source_matches = source_by_hash.get(target.sha256)
        if not source_matches:
            continue
        matched_hashes.add(target.sha256)
        matched_target_paths.add(target.relative_path)
        for source in source_matches:
            rows.append(
                {
                    "match_type": "exact_sha256",
                    "sha256": target.sha256,
                    "size_bytes": target.size_bytes,
                    "source_relative_path": source.relative_path,
                    "source_filename": source.filename,
                    "target_relative_path": target.relative_path,
                    "target_filename": target.filename,
                    "target_absolute_path": str(target.absolute_path),
                }
            )

    summary = {
        "source_total_files": len(source_records),
        "target_total_files": len(target_records),
        "matched_target_files": len(matched_target_paths),
        "matched_distinct_hashes": len(matched_hashes),
    }
    return rows, summary


def write_matches_csv(rows: list[dict[str, str | int]], output_path: Path) -> None:
    with output_path.open("w", newline="", encoding="utf-8") as handle:
        fieldnames = [
            "match_type",
            "sha256",
            "size_bytes",
            "source_relative_path",
            "source_filename",
            "target_relative_path",
            "target_filename",
            "target_absolute_path",
        ]
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def create_review_links(
    rows: list[dict[str, str | int]], review_dir: Path, link_mode: str
) -> list[dict[str, str]]:
    files_dir = review_dir / "files"
    files_dir.mkdir(parents=True, exist_ok=True)

    seen_targets: set[str] = set()
    review_items: list[dict[str, str]] = []
    index = 1

    for row in rows:
        target_path = str(row["target_absolute_path"])
        if target_path in seen_targets:
            continue
        seen_targets.add(target_path)

        source_rel = str(row["source_relative_path"])
        target_rel = str(row["target_relative_path"])
        filename = str(row["target_filename"])
        sha256 = str(row["sha256"])
        ext = Path(filename).suffix.lower()
        review_name = f"{index:06d}_{Path(filename).stem}{ext}"
        review_path = files_dir / review_name

        if review_path.exists():
            review_path.unlink()

        if link_mode == "symlink":
            os.symlink(target_path, review_path)
        elif link_mode == "hardlink":
            os.link(target_path, review_path)
        elif link_mode == "copy":
            shutil.copy2(target_path, review_path)
        else:
            raise ValueError(f"Unsupported link mode: {link_mode}")

        review_items.append(
            {
                "review_file": f"files/{review_name}",
                "source_relative_path": source_rel,
                "target_relative_path": target_rel,
                "target_absolute_path": target_path,
                "filename": filename,
                "sha256": sha256,
            }
        )
        index += 1

    return review_items


def write_review_gallery(review_items: list[dict[str, str]], output_path: Path) -> None:
    cards = []
    for item in review_items:
        media_tag = _media_tag(item["review_file"], item["filename"])
        cards.append(
            f"""
            <article class="card">
              <div class="media">{media_tag}</div>
              <div class="meta">
                <p><strong>{html.escape(item["filename"])}</strong></p>
                <p>Source: {html.escape(item["source_relative_path"])}</p>
                <p>Target: {html.escape(item["target_relative_path"])}</p>
                <p class="path">{html.escape(item["target_absolute_path"])}</p>
                <p class="hash">{html.escape(item["sha256"])}</p>
              </div>
            </article>
            """
        )

    page = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Photo Match Review</title>
  <style>
    :root {{
      color-scheme: light;
      --bg: #f6f2ea;
      --panel: #fffdf9;
      --ink: #1d1b18;
      --muted: #6f675d;
      --line: #d9cdbf;
      --accent: #8f3f2b;
    }}
    body {{
      margin: 0;
      font-family: Georgia, "Iowan Old Style", serif;
      background: linear-gradient(180deg, #f3eadf 0%, var(--bg) 100%);
      color: var(--ink);
    }}
    header {{
      padding: 24px;
      border-bottom: 1px solid var(--line);
      background: rgba(255, 253, 249, 0.92);
      position: sticky;
      top: 0;
      backdrop-filter: blur(10px);
    }}
    h1 {{
      margin: 0 0 6px;
      font-size: 28px;
    }}
    p {{
      margin: 0;
      color: var(--muted);
    }}
    main {{
      display: grid;
      grid-template-columns: repeat(auto-fill, minmax(320px, 1fr));
      gap: 18px;
      padding: 24px;
    }}
    .card {{
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 18px;
      overflow: hidden;
      box-shadow: 0 10px 30px rgba(64, 43, 25, 0.08);
    }}
    .media {{
      background: #efe2d2;
      aspect-ratio: 4 / 3;
      display: flex;
      align-items: center;
      justify-content: center;
    }}
    img, video {{
      max-width: 100%;
      max-height: 100%;
      display: block;
    }}
    .meta {{
      padding: 14px 16px 16px;
      font-size: 14px;
      line-height: 1.4;
    }}
    .meta strong {{
      color: var(--accent);
    }}
    .path, .hash {{
      word-break: break-word;
      font-family: Menlo, Monaco, monospace;
      font-size: 12px;
    }}
  </style>
</head>
<body>
  <header>
    <h1>Review Candidate Deletions</h1>
    <p>{len(review_items)} target-account files matched exactly against the source-account export.</p>
  </header>
  <main>
    {''.join(cards)}
  </main>
</body>
</html>
"""
    output_path.write_text(page, encoding="utf-8")


def _media_tag(review_file: str, filename: str) -> str:
    ext = Path(filename).suffix.lower()
    escaped = html.escape(review_file, quote=True)
    if ext in {".mp4", ".mov", ".m4v", ".avi", ".3gp", ".mts"}:
        return f'<video controls preload="metadata" src="{escaped}"></video>'
    return f'<img loading="lazy" src="{escaped}" alt="{html.escape(filename, quote=True)}">'


def ensure_empty_dir(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)


def cmd_build_manifest(args: argparse.Namespace) -> int:
    root = Path(args.root).expanduser().resolve()
    output = Path(args.output).expanduser().resolve()
    ensure_path_exists(root, "Export folder")
    records = build_manifest(root, label=root.name)
    output.parent.mkdir(parents=True, exist_ok=True)
    write_manifest_csv(records, output)
    print(f"Wrote manifest with {len(records)} files to {output}")
    return 0


def cmd_scan_library(args: argparse.Namespace) -> int:
    photos_db = Path(args.photos_db).expanduser().resolve() if args.photos_db else default_photos_library_path()
    output = Path(args.output).expanduser().resolve()
    ensure_path_exists(photos_db, "Photos database")
    records = build_library_manifest(photos_db)
    output.parent.mkdir(parents=True, exist_ok=True)
    write_library_manifest_csv(records, output)
    print(f"Wrote library manifest with {len(records)} assets to {output}")
    return 0


def cmd_compare_manifests(args: argparse.Namespace) -> int:
    out_dir = Path(args.out_dir).expanduser().resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    source_manifest = Path(args.source_manifest).expanduser().resolve()
    target_manifest = Path(args.target_manifest).expanduser().resolve()
    ensure_path_exists(source_manifest, "Source manifest")
    ensure_path_exists(target_manifest, "Target manifest")
    source_records = load_library_manifest_csv(source_manifest)
    target_records = load_library_manifest_csv(target_manifest)

    rows, summary = compare_library_manifests(source_records, target_records)
    write_library_matches_csv(rows, out_dir / "matched_target_assets.csv")
    write_target_uuid_list(rows, out_dir / "target_local_uuids.txt")
    (out_dir / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    write_library_review_html(rows, summary, out_dir / "review.html")

    print(json.dumps(summary, indent=2))
    print(f"Matches CSV: {out_dir / 'matched_target_assets.csv'}")
    print(f"Target UUID list: {out_dir / 'target_local_uuids.txt'}")
    print(f"Review HTML: {out_dir / 'review.html'}")
    return 0


def cmd_create_review_album(args: argparse.Namespace) -> int:
    uuid_file = Path(args.uuid_file).expanduser().resolve()
    ensure_path_exists(uuid_file, "UUID file")
    uuids = [
        line.strip()
        for line in uuid_file.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    if not uuids:
        print(f"No UUIDs found in {uuid_file}")
        return 0
    result = create_photos_review_album(uuids, args.album_name)
    print(result)
    return 0


def cmd_compare(args: argparse.Namespace) -> int:
    out_dir = Path(args.out_dir).expanduser().resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    if args.source_manifest:
        source_manifest = Path(args.source_manifest).expanduser().resolve()
        ensure_path_exists(source_manifest, "Source manifest")
        source_records = load_manifest_csv(source_manifest)
    else:
        source_root = Path(args.source_dir).expanduser().resolve()
        ensure_path_exists(source_root, "Source export folder")
        source_candidates = scan_export_candidates(source_root)
        print(
            f"Scanned {len(source_candidates)} source export files before hashing.",
            file=sys.stderr,
        )

    if args.target_manifest:
        target_manifest = Path(args.target_manifest).expanduser().resolve()
        ensure_path_exists(target_manifest, "Target manifest")
        target_records = load_manifest_csv(target_manifest)
    else:
        target_root = Path(args.target_dir).expanduser().resolve()
        ensure_path_exists(target_root, "Target export folder")
        target_candidates = scan_export_candidates(target_root)
        print(
            f"Scanned {len(target_candidates)} target export files before hashing.",
            file=sys.stderr,
        )

    if not args.source_manifest and not args.target_manifest:
        overlapping_keys = candidate_keys(source_candidates) & candidate_keys(target_candidates)
        source_candidates = filter_candidates_for_overlap(source_candidates, overlapping_keys)
        target_candidates = filter_candidates_for_overlap(target_candidates, overlapping_keys)
        print(
            f"Hashing {len(source_candidates)} source and {len(target_candidates)} target files after size/extension prefilter.",
            file=sys.stderr,
        )
        source_records = build_manifest_from_candidates(source_candidates, label="source")
        target_records = build_manifest_from_candidates(target_candidates, label="target")
        write_manifest_csv(source_records, out_dir / "source_manifest.csv")
        write_manifest_csv(target_records, out_dir / "target_manifest.csv")
    elif not args.source_manifest:
        source_records = build_manifest_from_candidates(source_candidates, label="source")
        write_manifest_csv(source_records, out_dir / "source_manifest.csv")
    elif not args.target_manifest:
        target_records = build_manifest_from_candidates(target_candidates, label="target")
        write_manifest_csv(target_records, out_dir / "target_manifest.csv")

    rows, summary = match_records(source_records, target_records)
    write_matches_csv(rows, out_dir / "matched_targets.csv")
    (out_dir / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")

    review_dir = out_dir / "review"
    ensure_empty_dir(review_dir)
    review_items = create_review_links(rows, review_dir, args.review_link_mode)
    write_review_gallery(review_items, review_dir / "index.html")
    (review_dir / "review_items.json").write_text(
        json.dumps(review_items, indent=2), encoding="utf-8"
    )

    print(json.dumps(summary, indent=2))
    print(f"Matches CSV: {out_dir / 'matched_targets.csv'}")
    print(f"Review gallery: {review_dir / 'index.html'}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Match likely duplicated Apple Photos assets across two libraries or exports."
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {VERSION}")
    subparsers = parser.add_subparsers(dest="command", required=True)

    scan_parser = subparsers.add_parser(
        "scan-library",
        help="Scan the local Apple Photos library database and write a metadata manifest.",
    )
    scan_parser.add_argument(
        "--photos-db",
        help="Path to Photos.sqlite. Defaults to ~/Pictures/Photos Library.photoslibrary/database/Photos.sqlite",
    )
    scan_parser.add_argument("--output", required=True, help="Output CSV path.")
    scan_parser.set_defaults(func=cmd_scan_library)

    compare_manifest_parser = subparsers.add_parser(
        "compare-manifests",
        help="Compare two library manifests from different machines/accounts.",
    )
    compare_manifest_parser.add_argument("--source-manifest", required=True, help="Source account manifest CSV.")
    compare_manifest_parser.add_argument("--target-manifest", required=True, help="Target account manifest CSV.")
    compare_manifest_parser.add_argument("--out-dir", required=True, help="Directory for outputs.")
    compare_manifest_parser.set_defaults(func=cmd_compare_manifests)

    album_parser = subparsers.add_parser(
        "create-review-album",
        help="Create a review album in Photos from a target_local_uuids.txt file.",
    )
    album_parser.add_argument("--uuid-file", required=True, help="Path to target_local_uuids.txt.")
    album_parser.add_argument(
        "--album-name",
        default="Photo Match Review",
        help="Album name to create or reuse in Photos.",
    )
    album_parser.set_defaults(func=cmd_create_review_album)

    manifest_parser = subparsers.add_parser(
        "build-manifest", help="Scan a folder and write a CSV manifest."
    )
    manifest_parser.add_argument("root", help="Folder containing exported photos/videos.")
    manifest_parser.add_argument("--output", required=True, help="Output CSV path.")
    manifest_parser.set_defaults(func=cmd_build_manifest)

    compare_parser = subparsers.add_parser(
        "compare",
        help="Compare source and target exports and generate a reviewable report.",
    )
    compare_group_source = compare_parser.add_mutually_exclusive_group(required=True)
    compare_group_source.add_argument("--source-dir", help="Source export folder.")
    compare_group_source.add_argument("--source-manifest", help="Existing source manifest CSV.")
    compare_group_target = compare_parser.add_mutually_exclusive_group(required=True)
    compare_group_target.add_argument("--target-dir", help="Target export folder.")
    compare_group_target.add_argument("--target-manifest", help="Existing target manifest CSV.")
    compare_parser.add_argument("--out-dir", required=True, help="Directory for outputs.")
    compare_parser.add_argument(
        "--review-link-mode",
        choices=["symlink", "hardlink", "copy"],
        default="symlink",
        help="How to materialize review files in the gallery directory.",
    )
    compare_parser.set_defaults(func=cmd_compare)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return args.func(args)
    except KeyboardInterrupt:
        print("Interrupted", file=sys.stderr)
        return 130
    except FileNotFoundError as exc:
        print(str(exc), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
