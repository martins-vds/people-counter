# Variable Library definitions

Export the reviewed Variable Library into `environment/` with the batch
manifest documented in [`fabric/README.md`](../README.md). Values that differ
by environment must be replaced with provider tokens during export; secrets
must remain in an approved secret store and must not be committed as library
defaults.

The generic exporter preserves the definition format returned by Fabric when
`format` is omitted. Validate the resulting bundle before wiring it into a
deployment resource.
