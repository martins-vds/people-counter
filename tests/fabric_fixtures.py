from uuid import UUID

from people_counter.fabric_target import FabricTarget

WORKSPACE_ID = str(UUID(int=1))
LAKEHOUSE_ID = str(UUID(int=2))
ENVIRONMENT_ID = str(UUID(int=3))
TARGET_IDS = {
    "workspace_id": WORKSPACE_ID,
    "lakehouse_id": LAKEHOUSE_ID,
    "environment_id": ENVIRONMENT_ID,
}
TEST_TARGET = FabricTarget(**TARGET_IDS)
SOURCE_MODEL_ID = str(UUID(int=99))
RUNTIME_CONTEXT = {
    "currentWorkspaceId": WORKSPACE_ID,
    "defaultLakehouseWorkspaceId": WORKSPACE_ID,
    "defaultLakehouseId": LAKEHOUSE_ID,
    "environmentWorkspaceId": WORKSPACE_ID,
    "environmentId": ENVIRONMENT_ID,
}
ROLE_IDS = {
    role: str(UUID(int=index))
    for index, role in enumerate(
        ("control", "dispatcher", "process", "refresh", "reconciliation", "gold"),
        start=10,
    )
}
