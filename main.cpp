#include <mpi.h>
#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <cstring>
#include <map>
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
    string directory = "chunks";

    // Check if the directory exists, and create it if it doesn't
    struct stat info;
    if (stat(directory.c_str(), &info) != 0) {
        if (mkdir(directory.c_str(), 0777) != 0) {
            cerr << "Error: Could not create directory!" << endl;
            return;
        }
    }

    string fileName = directory + "/node_" + to_string(rank) + "_" + chunkId + ".bin";
    ofstream file(fileName, ios::binary);

    if (file) {
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

    if (rank == 0) {
        // Rank 0: Metadata server
        map<string, map<string, vector<int>>> metadata; // Map<filename, Map<chunkId, replicas>>
        string command;

        while (true) {
            cout << "Enter command (upload <filename> / exit): ";
            getline(cin, command);

            if (command == "exit") {
                cout << "Exiting the program." << endl;
                break;
            }

            if (command.rfind("upload", 0) == 0) {
                string filePath = command.substr(7); // Extract the filename after "upload "

                if (filePath.empty()) {
                    cerr << "Error: No filename provided!" << endl;
                    continue;
                }

                size_t fileSize = getFileSize(filePath);
                if (fileSize == 0) {
                    cerr << "Error: File not found or empty!" << endl;
                    continue;
                }
                cout << "Rank 0: Reading file: " << filePath << ", Size: " << fileSize << " bytes." << endl;

                vector<string> chunks = partitionFile(filePath);
                if (chunks.empty()) {
                    cerr << "Error: Could not partition file!" << endl;
                    continue;
                }
                cout << "Rank 0: Partitioned file into " << chunks.size() << " chunks." << endl;

                map<string, vector<int>> chunkMetadata = distributeChunks(chunks, size - 1);
                metadata[filePath] = chunkMetadata;

                for (const auto &[chunkId, replicas] : chunkMetadata) {
                    for (int node : replicas) {
                        MPI_Send(chunkId.c_str(), chunkId.size() + 1, MPI_CHAR, node, 0, MPI_COMM_WORLD);
                        MPI_Send(chunks[stoi(chunkId.substr(6))].c_str(), chunks[stoi(chunkId.substr(6))].size() + 1, MPI_CHAR, node, 0, MPI_COMM_WORLD);
                    }
                }

                cout << "Rank 0: Uploaded file \"" << filePath << "\" successfully." << endl;
            } else {
                cerr << "Error: Unknown command!" << endl;
            }
        }
    } else {
        // Storage nodes (Rank 1 to N-1)
        while (true) {
            MPI_Status status;
            char chunkIdBuffer[50];

            MPI_Recv(chunkIdBuffer, 50, MPI_CHAR, 0, 0, MPI_COMM_WORLD, &status);
            string chunkId(chunkIdBuffer);

            char chunkDataBuffer[CHUNK_SIZE + 1];
            MPI_Recv(chunkDataBuffer, CHUNK_SIZE + 1, MPI_CHAR, 0, 0, MPI_COMM_WORLD, &status);
            string chunkData(chunkDataBuffer);

            storeChunk(chunkId, chunkData, rank);
        }
    }

    MPI_Finalize();
    return 0;
}

