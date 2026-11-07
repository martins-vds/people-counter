"""Prepare checked repository definitions for browser-only publication."""

import argparse
import hashlib
import json
from pathlib import Path

from people_counter.fabric_definition_bundle import read_definition_bundle


def prepare_assets(root: Path, output: Path) -> str:
    bundles = {}
    for relative in (
        "semantic-models/analytics",
        "semantic-models/operational",
        "reports/analytics",
        "reports/operational",
        "rti/queryset",
        "rti/dashboard",
        "activators/operations",
    ):
        bundles[relative] = read_definition_bundle(root / "fabric" / relative)["definition"]
    client_source = (root / "src/people_counter/fabric_rest.py").read_text()
    binding_import = "from people_counter.fabric_sjd import WORKSPACE_ID"
    if client_source.count(binding_import) != 1:
        raise ValueError("Fabric REST client workspace binding changed; review preparation")
    client_source = client_source.replace(binding_import, "WORKSPACE_ID = TARGET_WORKSPACE_ID")
    publication_source = (
        root / "src/people_counter/manual_fabric_publication.py"
    ).read_text()
    compile(client_source, "fabric_rest.py", "exec")
    compile(publication_source, "manual_fabric_publication.py", "exec")
    variables = root / "infra/modules/fabric-foundation/definitions/variable-library"
    document = {
        "schema": "people-counter-manual-publication-inputs-v1",
        "definitions": bundles,
        "clientSource": client_source,
        "publicationSource": publication_source,
        "variableSettings": json.loads((variables / "settings.json").read_text()),
        "variableTemplate": (variables / "variables.json.tmpl").read_text(),
    }
    content = json.dumps(document, sort_keys=True, separators=(",", ":")).encode()
    output.parent.mkdir(parents=True, exist_ok=True)
    if output.exists() and output.read_bytes() != content:
        raise ValueError(f"Refusing to overwrite different prepared inputs: {output}")
    output.write_bytes(content)
    return hashlib.sha256(content).hexdigest()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output", type=Path,
        default=Path("outputs/manual-fabric/manual-publication.json"),
    )
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    digest = prepare_assets(root, args.output)
    print(f"Prepared {args.output.resolve()}\nSHA256 {digest}")
    print("Upload through the Lakehouse UI. This command makes no cloud requests.")


if __name__ == "__main__":
    main()
