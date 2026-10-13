# Canonical Spark Job Definitions

Terraform reads these files for all seven production roles. Each role owns
`Main/main.py`. All roles use the metadata template under `common/`.

The Microsoft Fabric Terraform provider substitutes the target Lakehouse and
Environment IDs through its documented Go-template definition tokens. No
committed file contains an environment-specific artifact ID.

Tests compare these files with `build_sjd_definition()`. This check keeps each
generated payload equal to its Terraform source.
