#include <mpi.h>
#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <cstring>
#include <map>
#include <sys/stat.h>
#include <sys/stat.h>
#include <sys/types.h>




#define CHUNK_SIZE 32
#define REPLICATION_FACTOR 3

using namespace std;

// Function to get the file size
size_t getFileSize(const string &filePath) {
    struct stat fileStat;
    if (stat(filePath.c_str(), &fileStat) == 0) {
        return fileStat.st_size;
    }
    return 0;
}

// Function to read the file and partition it into chunks
vector<string> partitionFile(const string &filePath) {
    ifstream file(filePath, ios::binary);
    vector<string> chunks;

    if (!file) {
        cerr << "Error: File not found or cannot be opened!" << endl;
        return chunks;
    }

    char buffer[CHUNK_SIZE];
    while (file.read(buffer, CHUNK_SIZE) || file.gcount() > 0) {
        chunks.push_back(string(buffer, file.gcount()));
    }

    file.close();
    return chunks;
}

// Function to distribute chunks across nodes with replication
map<string, vector<int>> distributeChunks(const vector<string> &chunks, int numNodes) {
    map<string, vector<int>> metadata;

    for (size_t i = 0; i < chunks.size(); ++i) {
        vector<int> replicas;

        // Distribute replicas to distinct nodes using modulo arithmetic
        for (int r = 0; r < REPLICATION_FACTOR; ++r) {
            int node = (i + r) % numNodes + 1; // Nodes are from 1 to N-1
            replicas.push_back(node);
        }

        string chunkId = "Chunk_" + to_string(i);
        metadata[chunkId] = replicas;
    }

    return metadata;
}

void storeChunk(const string &chunkId, const string &chunkData, int rank) {
    // Define the directory path where the files will be stored
    string directory = "chunks";
    
    // Check if the directory exists, and create it if it doesn't
    struct stat info;
    if (stat(directory.c_str(), &info) != 0) {
        if (mkdir(directory.c_str(), 0777) != 0) {
            cerr << "Error: Could not create directory!" << endl;
            return;
        }
        cout << "Directory created: " << directory << endl;
    }

    // Create the file path with the chunk ID and rank
    string fileName = directory + "/node_" + to_string(rank) + "_" + chunkId + ".bin";
    
    // Open the file for writing in binary mode
    ofstream file(fileName, ios::binary);

    if (file) {
        // Write the chunk data to the file
        file.write(chunkData.c_str(), chunkData.size());
        file.close();
        cout << "Rank " << rank << " stored " << chunkId << " at " << fileName << "." << endl;
    } else {
        cerr << "Error: Could not store chunk at Rank " << rank << endl;
    }
}

int main(int argc, char *argv[]) {
    MPI_Init(&argc, &argv);

    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    if (size < 4) {
        if (rank == 0) {
            cerr << "Please run the program with at least 4 processes." << endl;
        }
        MPI_Finalize();
        return 0;
    }

    if (argc < 2) {
        if (rank == 0) {
            cerr << "Usage: mpirun -np <N> ./adfs <input_file>" << endl;
        }
        MPI_Finalize();
        return 0;
    }

    string filePath = argv[1];

    if (rank == 0) {
        // Rank 0: Metadata server (Iron Man)
        cout << "Rank 0: Reading input file: " << filePath << endl;

        // Step 1: Get file size
        size_t fileSize = getFileSize(filePath);
        if (fileSize == 0) {
            cerr << "Error: File not found or empty!" << endl;
            MPI_Finalize();
            return 0;
        }
        cout << "Rank 0: File size: " << fileSize << " bytes." << endl;

        // Step 2: Partition the file
        vector<string> chunks = partitionFile(filePath);
        if (chunks.empty()) {
            MPI_Finalize();
            return 0;
        }
        cout << "Rank 0: Partitioned file into " << chunks.size() << " chunks." << endl;

        for (size_t i = 0; i < chunks.size(); ++i) {
            cout << "Rank 0: Chunk " << i << " size: " << chunks[i].size() << " bytes." << endl;
        }

        // Step 3: Distribute chunks with replication
        map<string, vector<int>> metadata = distributeChunks(chunks, size - 1);
        cout << "Rank 0: Metadata (chunk-to-node mapping):" << endl;
        for (const auto &[chunkId, replicas] : metadata) {
            cout << chunkId << " -> ";
            for (int node : replicas) {
                cout << node << " ";
            }
            cout << endl;
        }

        // Step 4: Send chunks to storage nodes
        for (size_t i = 0; i < chunks.size(); ++i) {
            string chunkId = "Chunk_" + to_string(i);
            for (int node : metadata[chunkId]) {
                MPI_Send(chunkId.c_str(), chunkId.size() + 1, MPI_CHAR, node, 0, MPI_COMM_WORLD);
                MPI_Send(chunks[i].c_str(), chunks[i].size() + 1, MPI_CHAR, node, 0, MPI_COMM_WORLD);
            }
        }

    } else {
        // Rank 1 to N-1: Storage nodes (Avengers)
        while (true) {
            MPI_Status status;

            // Receive chunk ID
            char chunkIdBuffer[50];
            MPI_Recv(chunkIdBuffer, 50, MPI_CHAR, 0, 0, MPI_COMM_WORLD, &status);
            string chunkId(chunkIdBuffer);

            // Receive chunk data
            char chunkDataBuffer[CHUNK_SIZE + 1];
            MPI_Recv(chunkDataBuffer, CHUNK_SIZE + 1, MPI_CHAR, 0, 0, MPI_COMM_WORLD, &status);
            string chunkData(chunkDataBuffer);

            // Store the chunk
            storeChunk(chunkId, chunkData, rank);
        }
    }

    MPI_Finalize();
    return 0;
}
