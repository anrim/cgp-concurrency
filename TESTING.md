# Testing

Run the local spawn acceptance test on Tokio and Smol from the workspace root.

```sh
cargo test --locked -p cgp-concurrency-tests --all-features --test task_ownership
```

The same checked CGP caller holds an `Rc` across suspension and recovers the original allocation.
