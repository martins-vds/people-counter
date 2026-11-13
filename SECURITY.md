# Security policy

## Supported versions

Security fixes target the latest published People Counter release and the
`main` branch. Fixes are published in a subsequent release; older releases do
not receive backports. Upgrade to the latest release before checking whether
a previously reported vulnerability still affects your deployment.

See [releases](https://github.com/martins-vds/people-counter/releases) for the
latest published version.

## Reporting a vulnerability

Use GitHub's
[private vulnerability reporting](https://github.com/martins-vds/people-counter/security/advisories/new)
to report a suspected vulnerability. Private reporting is enabled for this
repository.

Do not report vulnerabilities through public issues, pull requests, or
discussions. Do not publish exploit details before coordinating disclosure
with the maintainers.

Include:

- The affected release or commit and component: SDK, operator portal,
  deployment scripts, infrastructure, or Fabric assets.
- Your runtime and deployment configuration, with sensitive values removed.
- Reproduction steps or a minimal proof of concept using synthetic data.
- The expected and observed behavior, impact, and any suggested mitigation.

Never attach credentials, access tokens, connection strings, private footage,
or identifying information about people in footage. Redact logs and screenshots.
Test only systems and data you own or have explicit permission to assess.
If credentials have been exposed, revoke or rotate them immediately; do not
wait for a project fix.

Maintainers coordinate investigation, remediation, and disclosure through the
private report. This project does not guarantee an acknowledgment or resolution
deadline.

## Deployment boundaries

The operator portal is a single trusted operator's localhost tool, not an
authenticated multi-user service. Do not expose it directly to the internet or
an untrusted network. See the [operator portal guide](docs/operator-portal.md)
for deployment constraints and cloud-action safeguards.

Keep footage, telemetry, portal state, and Azure credentials out of public
reports and source control. Use least-privilege Azure access and the protected
GitHub Environments described in the
[deployment guide](docs/github-deployment.md).

CodeQL and CI checks help detect issues, but passing checks does not establish
that a deployment is secure.
