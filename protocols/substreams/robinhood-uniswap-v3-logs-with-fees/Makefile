SUBSTREAMS_YAML=robinhood-uniswap-v3.yaml

.PHONY: build pack
build:
	cargo build --target wasm32-unknown-unknown --release -p robinhood-uniswap-v3-logs-with-fees

pack: build
	substreams pack $(SUBSTREAMS_YAML)
