# Hintsfile Generator

The [_SwiftSync_](https://gist.github.com/RubenSomsen/a61a37d14182ccd78760e477c78133cd) protocol requires a file of "hints" that represents the UTXO set at a given height. This is a binary to generate such hints. The program reads blocks from a Bitcoin Core data directory of choice, and computes the UTXO set hints to an output file. This file may then be distributed to users for initial block download.
