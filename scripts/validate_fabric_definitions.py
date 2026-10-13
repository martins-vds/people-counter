from __future__ import annotations

import argparse
import hashlib
import json
from collections.abc import Sequence
from pathlib import Path

from people_counter.fabric_definition_bundle import (
    BUNDLE_SCHEMA,
    METADATA_FILE,
    read_definition_bundle,
)


def _canonical_bytes(value: object) -> bytes:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()


def validate_tree(root: Path) -> dict[str, object]:
    if not root.is_dir():
        raise ValueError(f"definition root is not a directory: {root}")
    bundles = []
    for metadata_path in sorted(root.rglob(METADATA_FILE)):
        bundle_path = metadata_path.parent
        definition = read_definition_bundle(bundle_path)
        metadata_bytes = metadata_path.read_bytes()
        metadata = json.loads(metadata_bytes)
        bundles.append(
            {
                "bundleSha256": hashlib.sha256(metadata_bytes).hexdigest(),
                "contentSha256": metadata["contentSha256"],
                "format": definition["definition"]["format"],
                "itemType": metadata.get("itemType"),
                "path": bundle_path.relative_to(root).as_posix(),
                "parts": len(definition["definition"]["parts"]),
                "schema": metadata["schema"],
                "tokens": len(metadata["tokens"]),
            }
        )
    tree_sha = hashlib.sha256(_canonical_bytes(bundles)).hexdigest()
    return {
        "bundles": bundles,
        "count": len(bundles),
        "root": str(root),
        "schema": "people-counter-fabric-definition-validation-v1",
        "sourceBundleSchema": BUNDLE_SCHEMA,
        "treeSha256": tree_sha,
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("root", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--require-bundles", action="store_true")
    arguments = parser.parse_args(argv)
    result = validate_tree(arguments.root)
    if arguments.require_bundles and result["count"] == 0:
        parser.error(f"no definition bundles found under {arguments.root}")
    output = json.dumps(result, indent=2, sort_keys=True) + "\n"
    if arguments.output is None:
        print(output, end="")
    else:
        arguments.output.parent.mkdir(parents=True, exist_ok=True)
        arguments.output.write_text(output, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
