"""
Guards against the committed metadata/ drifting from scripts/generate_metadata.py.

metadata/ is generated from dataone.utils.schemas.ALL_TABLES. If a table is
added or changed there (or the generator changes) without re-running
`python scripts/generate_metadata.py`, this test fails and says which files differ.
"""

from __future__ import annotations

import json
import pathlib
import subprocess
import sys

REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
COMMITTED = REPO_ROOT / "metadata"


def _load_tree(root: pathlib.Path) -> dict[str, object]:
    return {
        str(path.relative_to(root).as_posix()): json.loads(path.read_text(encoding="utf-8"))
        for path in sorted(root.rglob("*.json"))
    }


def test_committed_metadata_matches_generator(tmp_path):
    subprocess.run(
        [
            sys.executable,
            str(REPO_ROOT / "scripts" / "generate_metadata.py"),
            "--output-dir",
            str(tmp_path),
        ],
        check=True,
        capture_output=True,
    )

    generated = _load_tree(tmp_path)
    committed = _load_tree(COMMITTED)

    missing = sorted(set(generated) - set(committed))
    stale = sorted(set(committed) - set(generated))
    changed = sorted(p for p in set(generated) & set(committed) if generated[p] != committed[p])

    assert not (missing or stale or changed), (
        "metadata/ is out of sync with scripts/generate_metadata.py. "
        "Re-run `python scripts/generate_metadata.py` and commit the result.\n"
        f"  missing from metadata/: {missing}\n"
        f"  stale (no longer generated): {stale}\n"
        f"  content differs: {changed}"
    )
