# Other Fabric item definitions

Store other supported Fabric item definitions in this directory. Add each item
to `fabric/definition-export-manifest.example.json`.

Use `items/<item-type>/<logical-name>` as the destination. Omit `format` unless
the API documents a selectable format. Define a token for each
environment-specific value.

The exporter does not infer support, invent definitions, or imply deployment
support. It preserves the live response and fails on API, format, token, path,
or integrity errors.
