# Photo Match Tool

`photo-match-tool` helps identify assets that were likely copied from one Apple Photos library into another.

It is designed for a cautious cleanup workflow:

1. Read metadata from each Photos library on separate Macs.
2. Compare the manifests.
3. Create a review album in the target library.
4. Manually inspect and delete in Photos.

The tool does not delete photos.

## What It Does

- Scans a local Apple Photos library database and writes a manifest CSV.
- Compares two manifests and finds likely duplicates in the target library.
- Creates a Photos album from the matched target-side UUIDs for manual review.
- Optionally compares exported originals by exact SHA-256 hash for a stricter second pass.

## Why This Exists

Apple Photos does not expose a supported public API for remotely diffing two iCloud Photos libraries. This tool uses local library data on macOS instead.

For the two-machine workflow it matches primarily on the Photos database field `ZORIGINALSTABLEHASH`, then falls back to a metadata key when that hash is missing. For exported originals it can do exact byte-for-byte SHA-256 matching.

## Safety

- The tool does not delete photos.
- The tool does not modify the Photos database.
- The optional `create-review-album` command only creates or reuses an album in Photos and adds matched items to it.
- Deleting in Photos is still manual and will sync through iCloud Photos on that account.

## Compatibility

- macOS
- Python 3.11+
- Apple Photos libraries stored locally

This relies on Apple Photos internal SQLite schema, which is not a public API and may change across macOS releases.

## Install

Run directly:

```bash
python3 photo_match_tool.py --help
```

Or install as a local CLI:

```bash
python3 -m pip install .
photo-match-tool --help
```

## Recommended Two-Machine Workflow

Keep account A signed in on one Mac and account B signed in on another.

On machine A:

```bash
python3 photo_match_tool.py scan-library --output ./account-a-manifest.csv
```

On machine B:

```bash
python3 photo_match_tool.py scan-library --output ./account-b-manifest.csv
```

Copy one manifest to the other machine, then compare:

```bash
python3 photo_match_tool.py compare-manifests \
  --source-manifest ./account-a-manifest.csv \
  --target-manifest ./account-b-manifest.csv \
  --out-dir ./compare-out
```

Create a review album in the target Photos library:

```bash
python3 photo_match_tool.py create-review-album \
  --uuid-file ./compare-out/target_local_uuids.txt \
  --album-name "Transferred From Old Account"
```

Then review that album in Photos and delete manually.

## Commands

### `scan-library`

Scans the local Photos library database and writes a manifest CSV.

Default database path:

```text
~/Pictures/Photos Library.photoslibrary/database/Photos.sqlite
```

Custom path example:

```bash
python3 photo_match_tool.py scan-library \
  --photos-db "/path/to/Some Library.photoslibrary/database/Photos.sqlite" \
  --output ./manifest.csv
```

### `compare-manifests`

Compares two manifest CSVs and writes:

- `matched_target_assets.csv`
- `target_local_uuids.txt`
- `review.html`
- `summary.json`

Match confidence:

- `original_stablehash`: high confidence
- `metadata_fallback`: medium confidence

The metadata fallback uses:

- original filename
- capture timestamp
- original file size
- dimensions
- duration
- media kind and subtype

### `create-review-album`

Reads `target_local_uuids.txt` and creates or reuses a Photos album containing the matched target-side assets.

### `build-manifest`

Scans an export folder and writes a CSV manifest with SHA-256 hashes.

### `compare`

Compares two exported-original folders, or two export manifests, by exact SHA-256 hash and generates a visual review gallery.

This is best used as a stricter follow-up check if you want byte-for-byte confirmation before final deletion.

## Export Verification Workflow

If you want an exact second pass:

1. Export `Unmodified Original` from the source account reference set.
2. Export `Unmodified Original` from the candidate set in the target account.
3. Compare the exports:

```bash
python3 photo_match_tool.py compare \
  --source-dir "/Volumes/External/exports/account-a" \
  --target-dir "/Volumes/External/exports/account-b-candidates" \
  --out-dir "./out"
```

This mode writes:

- `matched_targets.csv`
- `summary.json`
- `review/index.html`
- `review/files/`

## Limitations

- The two-machine workflow depends on Apple Photos internals, not a supported Apple API.
- `ZORIGINALSTABLEHASH` appears strong in practice but is not documented by Apple as a cross-library contract.
- Edited, transcoded, or recompressed assets may not match in exact-hash export mode.
- Metadata fallback matches should be reviewed more carefully than stable-hash matches.

## Publishing Notes

If you use this publicly, describe it as a local analysis and review tool. Do not describe it as using a supported Apple Photos API, because it does not.

## License

MIT
