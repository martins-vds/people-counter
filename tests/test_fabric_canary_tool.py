from __future__ import annotations

import base64
import builtins
import io
import json
import stat
import struct
import sys
import tempfile
import types
import unittest
import uuid
import zipfile
from collections.abc import Mapping
from contextlib import nullcontext
from pathlib import Path
from unittest.mock import patch

from people_counter.fabric_canary_tool import (
    DISPLAY_NAME,
    LAKEHOUSE_ID,
    MAIN_PATH,
    MANIFEST_PATH,
    METADATA_PATH,
    REQUIRED_RUN_ID,
    WORKSPACE_ID,
    AzureIdentityTokenProvider,
    AzureCliTokenProvider,
    CanaryError,
    DeploymentLock,
    DefinitionMismatchError,
    FabricClient,
    FabricHTTPError,
    HTTPResponse,
    PruneRefusedError,
    SecretScanError,
    StaticTokenProvider,
    assert_no_secrets,
    build_definition,
    build_parser,
    build_source_archive,
    build_source_bootstrap,
    canonical_json_bytes,
    decode_part,
    decoded_parts,
    definition_hash,
    inspect_source_archive,
    preflight_bundle,
    redact,
    refuse_unmanaged_prune,
    retained_libs,
    scan_decoded_parts,
    source_archive_from_bootstrap,
    validate_result,
    verify_definition,
)
from people_counter.fabric_runtime2_canary import (
    LocalCanaryStorage,
    RuntimeObservation,
    SAFETY_TOKEN,
    WRITE_SCOPE,
    installed_package_identity,
    run_local_analogue,
    validate_arguments,
)


ENVIRONMENT_ID = "7b722e4b-d503-4a7b-af9e-257aa30831a6"
ITEM_ID = "997755c5-6d3d-4b60-bcdc-cd819c5c64e0"
JOB_ID = "0123e998-cce2-4528-b5cc-07fd7199e550"
RUN_ID = "623e424f-ad8c-468b-908b-ad5c279d4095"


class FakeHTTP:
    def __init__(self, responses: list[HTTPResponse]) -> None:
        self.responses = list(responses)
        self.calls: list[dict[str, object]] = []

    def request(
        self,
        method: str,
        url: str,
        *,
        headers: Mapping[str, str],
        body: bytes | None,
        timeout: float,
    ) -> HTTPResponse:
        self.calls.append(
            {
                "method": method,
                "url": url,
                "headers": dict(headers),
                "body": body,
                "timeout": timeout,
            }
        )
        if not self.responses:
            raise AssertionError(f"Unexpected HTTP request: {method} {url}")
        return self.responses.pop(0)


def response(status: int, value: object = None, **headers: str) -> HTTPResponse:
    body = b"" if value is None else canonical_json_bytes(value)
    return HTTPResponse(status, headers, body)


def source_archive(main: bytes = b"print('canary')\n") -> bytes:
    memory = io.BytesIO()
    members = {
        "people_counter/__init__.py": b"",
        "people_counter/fabric_runtime2_canary.py": main,
        "people_counter/sjd_process.py": b"def execute_sjd_partition(rows): return rows\n",
        "people_counter/py.typed": b"",
        "people_counter-0.6.0.dist-info/METADATA": (
            b"Name: people-counter\nVersion: 0.6.0\n"
        ),
    }
    with zipfile.ZipFile(memory, "w", compression=zipfile.ZIP_STORED) as archive:
        for name, content in members.items():
            info = zipfile.ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))
            info.create_system = 3
            info.external_attr = (stat.S_IFREG | 0o600) << 16
            archive.writestr(info, content)
    return memory.getvalue()


def custom_source_archive(
    extras: list[tuple[zipfile.ZipInfo | str, bytes]],
    *,
    compression: int = zipfile.ZIP_STORED,
) -> bytes:
    memory = io.BytesIO()
    required: list[tuple[zipfile.ZipInfo | str, bytes]] = [
        ("people_counter/__init__.py", b""),
        ("people_counter/fabric_runtime2_canary.py", b"pass\n"),
        ("people_counter/sjd_process.py", b"pass\n"),
    ]
    with zipfile.ZipFile(memory, "w", compression=compression) as archive:
        for supplied, content in [*required, *extras]:
            if isinstance(supplied, zipfile.ZipInfo):
                info = supplied
            else:
                info = zipfile.ZipInfo(
                    supplied, date_time=(1980, 1, 1, 0, 0, 0)
                )
                info.compress_type = compression
                info.create_system = 3
                info.external_attr = (stat.S_IFREG | 0o600) << 16
            archive.writestr(info, content)
    return memory.getvalue()


def mutate_first_zip_member(
    archive: bytes,
    *,
    flag_bits: int | None = None,
    compress_type: int | None = None,
) -> bytes:
    changed = bytearray(archive)
    local = changed.index(b"PK\x03\x04")
    central = changed.index(b"PK\x01\x02")
    if flag_bits is not None:
        struct.pack_into("<H", changed, local + 6, flag_bits)
        struct.pack_into("<H", changed, central + 8, flag_bits)
    if compress_type is not None:
        struct.pack_into("<H", changed, local + 8, compress_type)
        struct.pack_into("<H", changed, central + 10, compress_type)
    return bytes(changed)


def payload(main: bytes = b"print('canary')\n", **kwargs: object) -> dict[str, object]:
    return build_definition(
        build_source_bootstrap(source_archive(main)),
        ENVIRONMENT_ID,
        source_commit="a" * 40,
        project_version="0.6.0",
        **kwargs,
    )


class DefinitionTests(unittest.TestCase):
    def test_exact_source_archive_clean_roundtrip_and_hashes(self) -> None:
        archive = source_archive()
        identity = inspect_source_archive(archive)
        extracted, observed = source_archive_from_bootstrap(
            build_source_bootstrap(archive)
        )
        self.assertEqual(extracted, archive)
        self.assertEqual(observed, identity)
        self.assertEqual(
            [member["path"] for member in identity["members"]],
            sorted(member["path"] for member in identity["members"]),
        )
        manifest = verify_definition(payload())
        self.assertEqual(manifest["sourceArchive"], identity)
        with tempfile.TemporaryDirectory(dir=Path.cwd()) as temporary:
            package = Path(temporary) / "people_counter"
            package.mkdir()
            for name in (
                "__init__.py",
                "fabric_runtime2_canary.py",
                "sjd_process.py",
            ):
                (package / name).write_text("pass\n")
            built_archive = build_source_archive(package)
            built_identity = inspect_source_archive(built_archive)
            self.assertIn(
                "people_counter/fabric_runtime2_canary.py",
                {member["path"] for member in built_identity["members"]},
            )

    def test_source_archive_scans_every_nested_secret(self) -> None:
        secrets = (
            (
                "people_counter/nested/password.py",
                b'password = "SYNTHETIC_TEST_PASSWORD_123"\n',
            ),
            (
                "people_counter/nested/token.py",
                b'access_token = "SYNTHETIC_TEST_TOKEN_123456"\n',
            ),
            (
                "people_counter/nested/key.py",
                b"-----BEGIN PRIVATE KEY-----\nTEST-ONLY\n",
            ),
        )
        for path, content in secrets:
            with self.subTest(path=path), self.assertRaises(SecretScanError):
                inspect_source_archive(custom_source_archive([(path, content)]))

    def test_source_archive_rejects_paths_duplicates_and_special_files(self) -> None:
        symlink = zipfile.ZipInfo(
            "people_counter/link.py", date_time=(1980, 1, 1, 0, 0, 0)
        )
        symlink.create_system = 3
        symlink.external_attr = (stat.S_IFLNK | 0o777) << 16
        cases = (
            [("/absolute.py", b"x")],
            [("people_counter/../escape.py", b"x")],
            [("people_counter\\escape.py", b"x")],
            [("people_counter//double.py", b"x")],
            [("people_counter/./dot.py", b"x")],
            [("people_counter/café.py", b"x")],
            [("people_counter/nul\x00.py", b"x")],
            [("other/root.py", b"x")],
            [("people_counter/key.pem", b"x")],
            [(symlink, b"target")],
            [("people_counter/__init__.py", b"duplicate")],
        )
        for extras in cases:
            with self.subTest(member=str(extras[0][0])), self.assertRaises(
                CanaryError
            ):
                inspect_source_archive(custom_source_archive(extras))
        directory = zipfile.ZipInfo(
            "people_counter/nested/", date_time=(1980, 1, 1, 0, 0, 0)
        )
        directory.create_system = 3
        directory.external_attr = (stat.S_IFDIR | 0o700) << 16
        with self.assertRaises(CanaryError):
            inspect_source_archive(custom_source_archive([(directory, b"")]))

    def test_source_archive_rejects_encryption_flags_and_compression(self) -> None:
        clean = custom_source_archive([])
        with self.assertRaisesRegex(CanaryError, "Encrypted"):
            inspect_source_archive(
                mutate_first_zip_member(clean, flag_bits=0x1)
            )
        with self.assertRaisesRegex(CanaryError, "flags"):
            inspect_source_archive(
                mutate_first_zip_member(clean, flag_bits=0x20)
            )
        with self.assertRaisesRegex(CanaryError, "compression"):
            inspect_source_archive(
                mutate_first_zip_member(clean, compress_type=99)
            )

    def test_source_archive_rejects_zip_bombs_and_limits(self) -> None:
        with self.assertRaises(CanaryError):
            inspect_source_archive(b"")
        bomb = custom_source_archive(
            [("people_counter/bomb.py", b"A" * 200_000)],
            compression=zipfile.ZIP_DEFLATED,
        )
        with self.assertRaisesRegex(CanaryError, "ratio"):
            inspect_source_archive(bomb)
        oversized = custom_source_archive(
            [("people_counter/large.py", b"A" * (1024 * 1024 + 1))]
        )
        with self.assertRaisesRegex(CanaryError, "expanded size"):
            inspect_source_archive(oversized)
        too_many = [
            (f"people_counter/members/m{index}.py", b"x")
            for index in range(257)
        ]
        with self.assertRaisesRegex(CanaryError, "member count"):
            inspect_source_archive(custom_source_archive(too_many))

    def test_source_bootstrap_rejects_tampering_and_opaque_source(self) -> None:
        bootstrap = build_source_bootstrap(source_archive())
        tampered = bootstrap.replace(b"_SOURCE_SHA256 = \"", b"_SOURCE_SHA256 = \"0")
        with self.assertRaises(CanaryError):
            source_archive_from_bootstrap(tampered)
        with self.assertRaisesRegex(CanaryError, "Opaque"):
            build_definition(
                b"print('opaque')\n",
                ENVIRONMENT_ID,
                source_commit="a" * 40,
                project_version="0.6.0",
            )

    def test_deterministic_complete_roundtrip_and_manifest(self) -> None:
        first = payload(libs={"b.py": b"B", "Libs/a.py": b"A"})
        second = payload(libs={"Libs/a.py": b"A", "Libs/b.py": b"B"})
        self.assertEqual(first, second)
        self.assertEqual(canonical_json_bytes(first), canonical_json_bytes(second))
        self.assertEqual(first["definition"]["format"], "SparkJobDefinitionV2")  # type: ignore[index]

        parts = decoded_parts(first)
        self.assertEqual(
            list(parts),
            sorted([METADATA_PATH, MAIN_PATH, "Libs/a.py", "Libs/b.py"]),
        )
        metadata = json.loads(parts[METADATA_PATH])
        self.assertEqual(metadata["defaultLakehouseArtifactId"], LAKEHOUSE_ID)
        self.assertEqual(metadata["environmentArtifactId"], ENVIRONMENT_ID)
        self.assertEqual(metadata["executableFile"], "main.py")
        self.assertEqual(
            metadata["additionalLibraryUris"],
            ["a.py", "b.py"],
        )
        self.assertIsNone(metadata["retryPolicy"])
        self.assertIn(
            f"--run-id {REQUIRED_RUN_ID}", metadata["commandLineArguments"]
        )
        self.assertIn("--safety-token PC_CANARY_ONLY_V1", metadata["commandLineArguments"])
        manifest = verify_definition(first)
        changed_main = payload(
            b"print('different')\n",
            libs={"Libs/a.py": b"A", "Libs/b.py": b"B"},
        )
        self.assertNotEqual(
            manifest["releaseDigest"],
            verify_definition(changed_main)["releaseDigest"],
        )
        self.assertEqual(manifest["schemaVersion"], 1)
        self.assertEqual(manifest["sourceCommit"], "a" * 40)
        self.assertEqual(manifest["projectVersion"], "0.6.0")
        self.assertEqual(manifest["workspaceId"], WORKSPACE_ID)
        self.assertEqual(manifest["displayName"], DISPLAY_NAME)
        self.assertEqual(len(definition_hash(first)), 64)
        self.assertEqual(retained_libs(first), {"Libs/a.py": b"A", "Libs/b.py": b"B"})

    def test_base64_is_canonical_and_decodes_binary(self) -> None:
        built = payload(libs={"data.bin": b"\x00\xff"})
        part = next(
            item
            for item in built["definition"]["parts"]  # type: ignore[index]
            if item["path"] == "Libs/data.bin"
        )
        self.assertEqual(part["payload"], base64.b64encode(b"\x00\xff").decode())
        self.assertEqual(decode_part(part), b"\x00\xff")

    def test_invalid_environment_and_reserved_manifest_are_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "UUID"):
            build_definition("pass", "not-a-uuid")
        with self.assertRaisesRegex(ValueError, "generated"):
            payload(libs={MANIFEST_PATH: b"overwrite"})

    def test_manifest_detects_decoded_tampering(self) -> None:
        built = payload()
        main = next(
            item
            for item in built["definition"]["parts"]  # type: ignore[index]
            if item["path"] == MAIN_PATH
        )
        main["payload"] = base64.b64encode(b"print('changed')").decode()
        with self.assertRaises(CanaryError):
            verify_definition(built)

        malformed = payload()
        main_part = next(
            item
            for item in malformed["definition"]["parts"]  # type: ignore[index]
            if item["path"] == MAIN_PATH
        )
        main_source = base64.b64decode(main_part["payload"])
        archive, _ = source_archive_from_bootstrap(main_source)
        manifest = verify_definition(malformed)
        manifest["schemaVersion"] = 2
        main_part["payload"] = base64.b64encode(
            build_source_bootstrap(archive, manifest)
        ).decode()
        with self.assertRaisesRegex(CanaryError, "schemaVersion"):
            verify_definition(malformed)


class ScannerAndPruneTests(unittest.TestCase):
    def test_scanner_true_positives_are_decoded(self) -> None:
        for source, expected_rule in (
            ("password = 'correct-horse-battery-staple'", "assigned-secret"),
            ("header = 'Bearer abcdefghijklmnopqrstuvwxyz'", "bearer-token"),
            ("# -----BEGIN PRIVATE KEY-----", "private-key"),
            ("url = '?sv=1&sig=abcdefghijklmno'", "sas-signature"),
            (
                "DefaultEndpointsProtocol=https;AccountName=x;EndpointSuffix=y",
                "connection-string",
            ),
        ):
            with self.subTest(rule=expected_rule):
                unsafe = payload()
                main = next(
                    item
                    for item in unsafe["definition"]["parts"]  # type: ignore[index]
                    if item["path"] == MAIN_PATH
                )
                main["payload"] = base64.b64encode(source.encode()).decode()
                findings = scan_decoded_parts(unsafe)
                self.assertIn(expected_rule, {item.rule for item in findings})
                with self.assertRaises(SecretScanError):
                    assert_no_secrets(unsafe)

    def test_scanner_false_positive_placeholders_and_environment_access(self) -> None:
        safe = payload(
            b"import os\npassword = os.environ['PASSWORD']\n"
            b"password = '__REQUIRED__'\nprint('Bearer')\n"
        )
        self.assertEqual(scan_decoded_parts(safe), [])

    def test_unmanaged_prune_refuses_and_retained_part_allows(self) -> None:
        live = payload(libs={"keep.py": b"important"})
        replacement = payload()
        with self.assertRaisesRegex(PruneRefusedError, "Libs/keep.py"):
            refuse_unmanaged_prune(live, replacement)
        retained = payload(libs=retained_libs(live))
        refuse_unmanaged_prune(live, retained)


class AuthAndRedactionTests(unittest.TestCase):
    def test_static_provider_and_redaction_never_reveal_token(self) -> None:
        token = "super-secret-token-material"
        provider = StaticTokenProvider(token)
        self.assertNotIn(token, repr(provider))
        message = redact(
            f"Authorization: Bearer {token} access_token={token}", (token,)
        )
        self.assertNotIn(token, message)
        self.assertGreaterEqual(message.count("<redacted>"), 2)

    def test_azure_identity_import_is_lazy_and_errors_fail_closed(self) -> None:
        provider = AzureIdentityTokenProvider("default")
        self.assertNotIn("azure.identity", sys.modules)
        real_import = builtins.__import__

        def reject_azure(name: str, *args: object, **kwargs: object) -> object:
            if name.startswith("azure"):
                raise ImportError("not installed")
            return real_import(name, *args, **kwargs)

        with patch("builtins.__import__", side_effect=reject_azure):
            with self.assertRaisesRegex(CanaryError, "azure-identity"):
                provider.get_token()
        self.assertNotIn("secret", repr(provider).lower().replace("<redacted>", ""))

    def test_all_azure_identity_modes_are_lazy_adapters(self) -> None:
        calls: list[tuple[object, ...]] = []

        class Credential:
            def __init__(self, *args: object, **kwargs: object) -> None:
                calls.append((*args, kwargs))

            def get_token(self, scope: str) -> object:
                calls.append(("scope", scope))
                return types.SimpleNamespace(token="identity-token")

        identity = types.ModuleType("azure.identity")
        identity.DefaultAzureCredential = Credential
        identity.ManagedIdentityCredential = Credential
        identity.ClientSecretCredential = Credential
        azure = types.ModuleType("azure")
        azure.__path__ = []  # type: ignore[attr-defined]
        with patch.dict(
            sys.modules, {"azure": azure, "azure.identity": identity}
        ):
            self.assertEqual(
                AzureIdentityTokenProvider("default").get_token(),
                "identity-token",
            )
            self.assertEqual(
                AzureIdentityTokenProvider(
                    "managed-identity", client_id="managed-client"
                ).get_token(),
                "identity-token",
            )
            self.assertEqual(
                AzureIdentityTokenProvider(
                    "service-principal",
                    tenant_id="tenant",
                    client_id="client",
                    client_secret="credential-secret",
                ).get_token(),
                "identity-token",
            )
        self.assertIn(({"client_id": "managed-client"},), calls)
        self.assertIn(
            ("tenant", "client", "credential-secret", {}),
            calls,
        )

    def test_azure_cli_token_is_captured_not_logged(self) -> None:
        completed = types.SimpleNamespace(stdout="cli-token\n")
        with patch("subprocess.run", return_value=completed) as run:
            self.assertEqual(AzureCliTokenProvider().get_token(), "cli-token")
        command = run.call_args.args[0]
        self.assertEqual(command[:3], ["az", "account", "get-access-token"])
        self.assertTrue(run.call_args.kwargs["capture_output"])

    def test_http_error_redacts_response_token(self) -> None:
        token = "a-token-that-must-not-leak"
        transport = FakeHTTP(
            [response(401, {"error": f"Bearer {token}", "access_token": token})]
        )
        client = FabricClient(StaticTokenProvider(token), transport=transport)
        with self.assertRaises(FabricHTTPError) as caught:
            client.list_definitions()
        self.assertNotIn(token, str(caught.exception))
        self.assertEqual(
            transport.calls[0]["headers"]["Authorization"],  # type: ignore[index]
            f"Bearer {token}",
        )


class DeploymentLockTests(unittest.TestCase):
    def test_lock_uses_local_thread_and_advisory_file_lock(self) -> None:
        calls: list[object] = []

        class FakeFile:
            def fileno(self) -> int:
                return 42

            def close(self) -> None:
                calls.append("close")

        class FakeParent:
            def mkdir(self, **kwargs: object) -> None:
                calls.append(("mkdir", kwargs))

        class FakePath:
            parent = FakeParent()

            def resolve(self) -> object:
                return self

            def open(self, mode: str) -> FakeFile:
                calls.append(("open", mode))
                return FakeFile()

            def __str__(self) -> str:
                return "in-memory-lock"

        fcntl = types.ModuleType("fcntl")
        fcntl.LOCK_EX = 1
        fcntl.LOCK_NB = 2
        fcntl.LOCK_UN = 4
        fcntl.flock = lambda descriptor, flags: calls.append(
            ("flock", descriptor, flags)
        )
        with (
            patch.dict(sys.modules, {"fcntl": fcntl}),
            patch("os.chmod") as chmod,
        ):
            with DeploymentLock(FakePath()):  # type: ignore[arg-type]
                calls.append("held")
        self.assertIn(("flock", 42, 3), calls)
        self.assertIn(("flock", 42, 4), calls)
        self.assertLess(calls.index(("flock", 42, 3)), calls.index("held"))
        chmod.assert_called_once_with(unittest.mock.ANY, 0o600)


class FabricEndpointTests(unittest.TestCase):
    def client(self, responses: list[HTTPResponse], **kwargs: object) -> tuple[FabricClient, FakeHTTP]:
        transport = FakeHTTP(responses)
        return (
            FabricClient(
                StaticTokenProvider("token"),
                transport=transport,
                sleep=lambda _: None,
                lock_factory=nullcontext,
                **kwargs,
            ),
            transport,
        )

    def test_list_get_definition_status_and_cancel_endpoints(self) -> None:
        built = payload()
        client, transport = self.client(
            [
                response(200, {"value": [{"id": ITEM_ID, "displayName": DISPLAY_NAME}]}),
                response(200, built),
                response(200, {"status": "Running"}),
                response(200, {"status": "Cancelled"}),
            ]
        )
        self.assertEqual(client.find_canary()["id"], ITEM_ID)  # type: ignore[index]
        self.assertEqual(client.get_definition(ITEM_ID), built)
        self.assertEqual(client.status(ITEM_ID, JOB_ID)["status"], "Running")
        self.assertEqual(client.cancel(ITEM_ID, JOB_ID)["status"], "Cancelled")
        paths = [call["url"] for call in transport.calls]
        self.assertTrue(
            paths[0].endswith(
                f"/workspaces/{WORKSPACE_ID}/items?type=SparkJobDefinition"
            )
        )
        self.assertTrue(
            paths[1].endswith(
                f"/sparkJobDefinitions/{ITEM_ID}/getDefinition"
                "?format=SparkJobDefinitionV2"
            )
        )
        self.assertTrue(paths[2].endswith(f"/items/{ITEM_ID}/jobs/instances/{JOB_ID}"))
        self.assertTrue(paths[3].endswith(f"/jobs/instances/{JOB_ID}/cancel"))
        self.assertEqual(transport.calls[1]["method"], "POST")

    def test_lro_honors_retry_after_and_resource_location(self) -> None:
        sleeps: list[float] = []
        transport = FakeHTTP(
            [
                response(202, {}, Location="https://fabric.example/operations/op", **{"Retry-After": "3"}),
                response(202, {"status": "Running"}, **{"Retry-After": "2"}),
                response(
                    200,
                    {
                        "status": "Succeeded",
                        "resourceLocation": "https://fabric.example/resources/item",
                    },
                ),
                response(200, {"id": ITEM_ID}),
            ]
        )
        client = FabricClient(
            StaticTokenProvider("token"),
            transport=transport,
            sleep=sleeps.append,
            monotonic=lambda: 0,
        )
        result = client.create_definition(payload())
        self.assertEqual(result, {"id": ITEM_ID})
        self.assertEqual(sleeps, [3.0, 2.0])
        self.assertEqual(transport.calls[-1]["url"], "https://fabric.example/resources/item")

    def test_lro_retries_throttling_and_fails_on_terminal_error(self) -> None:
        client, transport = self.client(
            [
                response(202, {}, Location="https://fabric.example/operations/op"),
                response(429, {"error": "throttled"}, **{"Retry-After": "0"}),
                response(200, {"status": "Failed", "error": "bad definition"}),
            ]
        )
        with self.assertRaisesRegex(FabricHTTPError, "ended with status"):
            client.create_definition(payload())
        self.assertEqual(len(transport.calls), 3)

    def test_get_definition_strips_lro_metadata_and_nested_result(self) -> None:
        built = payload()
        service_definition = json.loads(json.dumps(built))
        del service_definition["definition"]["format"]
        client, _ = self.client(
            [
                response(
                    202,
                    {},
                    **{"x-ms-operation-id": "definition-operation", "Retry-After": "0"},
                ),
                response(
                    200,
                    {"status": "Succeeded", "result": service_definition},
                ),
            ]
        )
        self.assertEqual(client.get_definition(ITEM_ID), built)

    def test_update_body_is_complete_replacement(self) -> None:
        built = payload()
        client, transport = self.client([response(200, {"status": "Succeeded"})])
        client.update_definition(ITEM_ID, built)
        sent = json.loads(transport.calls[0]["body"])  # type: ignore[arg-type]
        self.assertEqual(sent["definition"], built["definition"])
        self.assertEqual(set(sent), {"definition"})
        self.assertTrue(
            str(transport.calls[0]["url"]).endswith(
                f"/sparkJobDefinitions/{ITEM_ID}/updateDefinition"
            )
        )

    def test_run_uses_fresh_uuid_and_exact_argument_override(self) -> None:
        built = payload()
        client, transport = self.client(
            [response(200, built), response(202, {"id": JOB_ID})]
        )
        submission = client.run(ITEM_ID)
        uuid.UUID(submission.run_id)
        sent = json.loads(transport.calls[1]["body"])  # type: ignore[arg-type]
        execution = sent["executionData"]
        self.assertIn(f"--run-id {submission.run_id}", execution["commandLineArguments"])
        self.assertEqual(set(execution), {"commandLineArguments"})
        self.assertIn("--project-version 0.6.0", execution["commandLineArguments"])
        self.assertTrue(
            str(transport.calls[1]["url"]).endswith(
                f"/sparkJobDefinitions/{ITEM_ID}/jobs/sparkjob/instances"
            )
        )

    def test_run_202_returns_immediately_and_cancel_accepts_empty_202(self) -> None:
        location = (
            f"https://api.fabric.microsoft.com/v1/workspaces/{WORKSPACE_ID}/"
            f"items/{ITEM_ID}/jobs/instances/{JOB_ID}"
        )
        client, transport = self.client(
            [
                response(200, payload()),
                response(202, None, Location=location),
                response(202),
            ]
        )
        submission = client.run(ITEM_ID, run_id=RUN_ID)
        self.assertEqual(submission.job_instance_id, JOB_ID)
        cancelled = client.cancel(ITEM_ID, JOB_ID)
        self.assertEqual(cancelled["status"], "CancelRequested")
        self.assertEqual(len(transport.calls), 3)

    def test_run_accepts_only_uuid_override(self) -> None:
        client, _ = self.client(
            [response(200, payload()), response(202, {"id": JOB_ID})]
        )
        self.assertEqual(client.run(ITEM_ID, run_id=RUN_ID).run_id, RUN_ID)
        with self.assertRaisesRegex(ValueError, "UUID"):
            client.run(ITEM_ID, run_id=REQUIRED_RUN_ID)

    def test_environment_verification_when_fields_are_available(self) -> None:
        client, transport = self.client(
            [
                response(200, {"properties": {"publishState": "Published"}}),
                response(200, {"runtimeVersion": "2.0"}),
            ]
        )
        verified = client.verify_environment(ENVIRONMENT_ID)
        self.assertEqual(
            verified["publishedSparkCompute"], {"runtimeVersion": "2.0"}
        )
        self.assertTrue(
            str(transport.calls[1]["url"]).endswith(
                f"/environments/{ENVIRONMENT_ID}/sparkcompute?beta=false"
            )
        )
        client, _ = self.client(
            [
                response(200, {"properties": {"publishState": "Draft"}}),
                response(200, {"runtimeVersion": "2.0"}),
            ]
        )
        with self.assertRaisesRegex(CanaryError, "not published"):
            client.verify_environment(ENVIRONMENT_ID)
        client, _ = self.client(
            [
                response(200, {"properties": {"publishState": "Published"}}),
                response(200, {"runtimeVersion": "1.3"}),
            ]
        )
        with self.assertRaisesRegex(CanaryError, "2.0"):
            client.verify_environment(ENVIRONMENT_ID)

    def test_wait_status_honors_retry_after_and_extracts_failure(self) -> None:
        sleeps: list[float] = []
        transport = FakeHTTP(
            [
                response(200, {"status": "Running"}, **{"Retry-After": "2"}),
                response(
                    200,
                    {"status": "Failed", "failureReason": {"message": "boom"}},
                ),
            ]
        )
        client = FabricClient(
            StaticTokenProvider("token"),
            transport=transport,
            sleep=sleeps.append,
            monotonic=lambda: 0,
        )
        result = client.wait_status(ITEM_ID, JOB_ID, poll_timeout=10)
        self.assertEqual([call["method"] for call in transport.calls], ["GET", "GET"])
        self.assertFalse(result["succeeded"])
        self.assertTrue(result["terminal"])
        self.assertEqual(
            set(result),
            {"status", "failureReason", "terminal", "succeeded", "failure"},
        )
        self.assertEqual(result["failure"], {"message": "boom"})
        self.assertEqual(sleeps, [2.0])

        client, _ = self.client([response(200, {"status": "Completed"})])
        completed = client.wait_status(ITEM_ID, JOB_ID, poll_timeout=10)
        self.assertTrue(completed["terminal"])
        self.assertTrue(completed["succeeded"])

        client, _ = self.client(
            [response(200, {"status": "Failed", "error": {"code": "bad"}})]
        )
        failed = client.wait_status(ITEM_ID, JOB_ID, poll_timeout=10)
        self.assertEqual(failed["failure"], {"code": "bad"})

        client, _ = self.client([response(200, {"status": "Failed"})])
        failed = client.wait_status(ITEM_ID, JOB_ID, poll_timeout=10)
        self.assertEqual(
            failed["failure"], "Fabric job failed without failure details"
        )

        throttled, throttle_transport = self.client(
            [
                response(429, {"status": "Throttled"}, **{"Retry-After": "1"}),
                response(200, {"status": "Completed"}),
            ]
        )
        throttle_sleeps: list[float] = []
        throttled.sleep = throttle_sleeps.append
        throttled.monotonic = lambda: 0
        self.assertTrue(
            throttled.wait_status(
                ITEM_ID, JOB_ID, poll_timeout=10
            )["succeeded"]
        )
        self.assertEqual(throttle_sleeps, [1.0])
        self.assertEqual(
            [call["method"] for call in throttle_transport.calls],
            ["GET", "GET"],
        )

        times = iter((0.0, 1.0))
        timeout_client, _ = self.client(
            [response(200, {"status": "Running"})]
        )
        timeout_client.monotonic = lambda: next(times)
        with self.assertRaises(TimeoutError):
            timeout_client.wait_status(ITEM_ID, JOB_ID, poll_timeout=1)

    def test_deploy_updates_under_lock_and_requires_exact_readback(self) -> None:
        built = payload()
        responses = [
            response(200, {"properties": {"publishState": "Published"}}),
            response(200, {"runtimeVersion": "2.0"}),
            response(200, {"value": [{"id": ITEM_ID, "displayName": DISPLAY_NAME}]}),
            response(200, built),
            response(200, {"status": "Succeeded"}),
            response(200, built),
        ]
        client, _ = self.client(responses)
        result = client.deploy(built)
        self.assertEqual(result["itemId"], ITEM_ID)
        self.assertEqual(result["definitionSha256"], definition_hash(built))

        changed = payload(b"print('different')\n")
        responses = [
            response(200, {}),
            response(200, {"runtimeVersion": "2.0"}),
            response(200, {"value": [{"id": ITEM_ID, "displayName": DISPLAY_NAME}]}),
            response(200, built),
            response(200, {}),
            response(200, changed),
        ]
        client, _ = self.client(responses)
        with self.assertRaises(DefinitionMismatchError):
            client.deploy(built)

    def test_deploy_create_path_reads_back_complete_definition(self) -> None:
        built = payload()
        client, transport = self.client(
            [
                response(200, {"publishDetails": {"state": "Succeeded"}}),
                response(200, {"environment": {"runtimeVersion": "2.0"}}),
                response(200, {"value": []}),
                response(201, {"id": ITEM_ID}),
                response(200, built),
            ]
        )
        result = client.deploy(built)
        self.assertEqual(result["itemId"], ITEM_ID)
        create_body = json.loads(transport.calls[3]["body"])  # type: ignore[arg-type]
        self.assertNotIn("type", create_body)
        self.assertEqual(create_body["displayName"], DISPLAY_NAME)
        self.assertEqual(
            create_body["definition"]["format"], "SparkJobDefinitionV2"
        )


class BundleAndCliTests(unittest.TestCase):
    def test_cp313_linux_bundle_and_pure_wheel_pass(self) -> None:
        names = preflight_bundle(
            [
                "people_counter-0.6.0-py3-none-any.whl",
                "numpy-2.0.0-cp313-cp313-manylinux_2_28_x86_64.whl",
            ]
        )
        self.assertEqual(len(names), 2)

        memory = io.BytesIO()
        with zipfile.ZipFile(memory, "w") as archive:
            archive.writestr(
                "bundle/pkg-1.0-cp313-cp313-manylinux_2_28_x86_64.whl", b"x"
            )
        self.assertEqual(len(preflight_bundle(memory.getvalue())), 1)

    def test_bundle_rejects_cp312_and_non_linux(self) -> None:
        for name in (
            "numpy-2.0-cp312-cp312-manylinux_2_28_x86_64.whl",
            "numpy-2.0-cp313-cp313-win_amd64.whl",
        ):
            with self.subTest(name=name), self.assertRaises(CanaryError):
                preflight_bundle([name])

    def test_help_and_all_subcommand_help_need_no_optional_dependencies(self) -> None:
        parser = build_parser()
        with self.assertRaises(SystemExit) as top:
            parser.parse_args(["--help"])
        self.assertEqual(top.exception.code, 0)
        for command in (
            "build",
            "inspect-live",
            "deploy",
            "run",
            "status",
            "cancel",
            "validate-result",
        ):
            with self.subTest(command=command), self.assertRaises(SystemExit) as help_exit:
                parser.parse_args([command, "--help"])
            self.assertEqual(help_exit.exception.code, 0)

    def test_validate_result_is_fail_closed(self) -> None:
        package_identity = installed_package_identity()
        arguments = validate_arguments(
            WORKSPACE_ID,
            LAKEHOUSE_ID,
            RUN_ID,
            WRITE_SCOPE,
            SAFETY_TOKEN,
            "a" * 64,
            "0.6.0",
            package_identity,
            "f" * 64,
            "d" * 64,
        )
        runtime = RuntimeObservation(
            python="3.13.7",
            spark="4.1.1",
            java="21.0.8",
            scala="2.13.16",
            delta="4.2.0",
            package_version="0.6.0",
            release_digest="a" * 64,
        )
        with tempfile.TemporaryDirectory(dir=Path.cwd()) as temporary:
            storage = LocalCanaryStorage(Path(temporary))
            run_local_analogue(storage, arguments, runtime=runtime)
            root = Path(temporary) / arguments.relative_run_root
            artifacts = {
                "result": json.loads((root / "result.json").read_text()),
                "success": json.loads((root / "_SUCCESS").read_text()),
                "pointer": json.loads((root / "pointer.json").read_text()),
                "records": json.loads(
                    (root / "attempts/records.json").read_text()
                ),
                "pointer_rows": [
                    json.loads((root / "pointer_delta/row.json").read_text())
                ],
            }
            expected = {
                "expected_run_id": RUN_ID,
                "expected_release_digest": "a" * 64,
                "expected_definition_hash": "f" * 64,
                "expected_package_identity": package_identity,
                "expected_source_archive_sha256": "d" * 64,
                "expected_project_version": "0.6.0",
            }
            self.assertEqual(
                validate_result(
                    artifacts["result"],
                    success=artifacts["success"],
                    pointer=artifacts["pointer"],
                    records=artifacts["records"],
                    pointer_rows=artifacts["pointer_rows"],
                    **expected,
                )["status"],
                "SUCCEEDED",
            )
            cases = {
                "wrong-definition": ("result", "definitionSha256", "0" * 64),
                "wrong-workspace": ("result", "workspaceId", str(uuid.uuid4())),
                "wrong-release": ("result", "releaseDigest", "0" * 64),
                "wrong-status": ("result", "status", "RUNNING"),
                "wrong-count": ("success", "recordCount", 999),
                "wrong-staging": ("pointer", "stagingIdentity", {}),
                "wrong-runtime": ("result", "runtime", {}),
                "wrong-provenance": ("result", "provenance", {}),
            }
            for name, (artifact_name, key, value) in cases.items():
                changed = json.loads(json.dumps(artifacts))
                changed[artifact_name][key] = value
                with self.subTest(name=name), self.assertRaises(CanaryError):
                    validate_result(
                        changed["result"],
                        success=changed["success"],
                        pointer=changed["pointer"],
                        records=changed["records"],
                        pointer_rows=changed["pointer_rows"],
                        **expected,
                    )
            changed = json.loads(json.dumps(artifacts))
            changed["records"][0]["status"] = "FAILED"
            with self.assertRaises(CanaryError):
                validate_result(
                    changed["result"],
                    success=changed["success"],
                    pointer=changed["pointer"],
                    records=changed["records"],
                    pointer_rows=changed["pointer_rows"],
                    **expected,
                )
            with self.assertRaises(CanaryError):
                validate_result(
                    artifacts["result"],
                    success=artifacts["success"],
                    pointer=artifacts["pointer"],
                    records=artifacts["records"],
                    pointer_rows=[],
                    **expected,
                )


if __name__ == "__main__":
    unittest.main()
