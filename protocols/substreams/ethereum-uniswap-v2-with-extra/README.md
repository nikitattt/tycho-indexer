# Ethereum Uniswap V2 With Extra

This package wraps `ethereum-uniswap-v2` and adds storage-based V2 pair attributes for
existing pool components.

It exposes:

- `map_v2_extra_changes`: emits only `k_last` and `total_supply` updates.
- `map_pool_events_with_extra`: emits the parent V2 output plus the extra attributes.

The extra attributes are written on the normal V2 pool component ID, using the raw pool address:

- `total_supply`
- `k_last`

The Base manifest imports the parent V2 package as a pinned `.spkg`:

```yaml
imports:
    v2: ./spkg/base-uniswap-v2-v0.3.2.spkg
```

For historical backfill, configure Tycho with a temporary extractor using:

```yaml
module_name: "map_v2_extra_changes"
```

That mode emits no component creations and updates only existing V2 pool components.
