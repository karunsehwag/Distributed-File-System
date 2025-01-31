# Distributed File System (DFS)

## Overview
In this project, we design and implement a robust Distributed File System (DFS) that ensures redundancy, reliability, and resilience. The system consists of a central metadata server that manages file metadata and various storage nodes responsible for storing file chunks. The DFS supports file uploads, downloads, and searches with built-in load balancing and replication. Additionally, it implements failover and recovery mechanisms to handle storage node failures.

## System Components
- **Metadata Server (Rank 0):** Manages metadata and indexing for file chunks.
- **Storage Nodes (Ranks 1 to N-1):** Store file chunks and handle retrieval requests.
- **Fault Tolerance:** Includes heartbeat monitoring, failover, and recovery features.

## Features
### 1. File Partitioning and Three-Way Replication
- Files are split into fixed-size chunks (32 bytes each).
- Each chunk is replicated across three distinct storage nodes.
- No requirement to maintain the replication factor in case of failures.

### 2. Load Balancing
- Distributes chunks evenly across storage nodes.
- Ensures that no node holds more than one replica of the same chunk.

### 3. Heartbeat Mechanism
- Each storage node periodically sends a heartbeat to the metadata server.
- If a node fails to send a heartbeat for more than 3 seconds, it is marked as down.
- A separate thread is used to handle heartbeats.

### 4. Failover and Recovery Operations
- **Failover:** Simulates storage node failure by stopping requests and heartbeat signals.
- **Recovery:** Restores failed nodes, allowing them to resume operations.

### 5. File Operations
- **Upload:** Splits files into chunks and distributes them with replication.
- **Retrieve:** Retrieves chunks and reassembles the file.
- **Search:** Locates offsets containing a specific word within a file.

### 6. Error Handling
- Outputs `-1` in case of errors (e.g., missing files, unavailable chunks, or invalid operations).

## Input Format
Commands accepted via stdin:
- `upload file_name absolute_path`
- `retrieve file_name`
- `search file_name word`
- `list file file_name`
- `failover rank`
- `recover rank`
- `exit`

## Output Format
- `1` for successful operations.
- `-1` for errors.
- Specific output format for `list file` and `search` commands as described in requirements.

## Execution Considerations
- Minimum of 4 processes required for execution.
- The metadata server is always available (0 downtime).
- Operations are designed to function even with node failures (within limits).


