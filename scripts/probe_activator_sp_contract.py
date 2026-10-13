from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence

from people_counter.fabric_activator import (
    ActivatorAutomationError,
    probe_service_principal_crud_contract,
)
from people_counter.fabric_automation import FabricApiError, FabricAutomationClient
from people_counter.fabric_rest import AzureCliTokenProvider, UrllibTransport


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Create, read, update, list, and hard-delete a disposable Reflex "
            "using the current Azure CLI identity."
        )
    )
    parser.add_argument("--workspace-id", required=True)
    parser.add_argument(
        "--api-root", default="https://api.fabric.microsoft.com/v1"
    )
    arguments = parser.parse_args(argv)
    client = FabricAutomationClient(
        AzureCliTokenProvider(),
        transport=UrllibTransport(),
        api_root=arguments.api_root,
    )
    try:
        evidence = probe_service_principal_crud_contract(
            client, workspace_id=arguments.workspace_id
        )
    except (ActivatorAutomationError, FabricApiError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 1
    print(evidence.to_json())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
