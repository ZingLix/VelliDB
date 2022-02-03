cargo run  -- --config-file ./config.toml --node-id 1 --address 0.0.0.0:38808 --store-path ./tmp/server1 &
cargo run  -- --config-file ./config.toml --node-id 2 --address 0.0.0.0:38809 --store-path ./tmp/server2 &
cargo run  -- --config-file ./config.toml --node-id 3 --address 0.0.0.0:38810 --store-path ./tmp/server3 &
wait
