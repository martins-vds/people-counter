# Other Fabric item definitions

Items not assigned a first-class directory may be exported here when their
public Fabric API supports `getDefinition`. Add an entry to
`fabric/migration-manifest.example.json`, use a destination of
`items/<item-type>/<logical-name>`, omit `format` unless the API documents a
specific selectable format, and declare every environment-specific literal as
an environment-backed token.

The exporter does not infer support, invent definitions, or imply deployment
support. It preserves the live response and fails on API, format, token, path,
or integrity errors.
