# Canonical Spark Job Definitions

Terraform reads these source-controlled payloads for all seven production roles.
Each role owns `Main/main.py`; all roles share the metadata template under
`common/`.

The Microsoft Fabric Terraform provider substitutes the target Lakehouse and
Environment IDs through its documented Go-template definition tokens. No
environment-specific artifact ID is committed here.

Tests compare these files with `build_sjd_definition()` so the Terraform and
legacy REST deployment paths cannot drift silently.
