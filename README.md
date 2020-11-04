# :bee: Buzz Rust implem :bee:

Welcome to the Rust implementation of Buzz. Buzz is a serverless query engine. The Rust implementation is based on Apache Arrow and the DataFusion engine.

## Architecture

Buzz is composed of two systems:
- :bee: the bees: cloud function workers that fetch data from the cloud storage and pre-aggregate it
- :honey_pot: the hives: container based reducers that collect the aggregates from the bees