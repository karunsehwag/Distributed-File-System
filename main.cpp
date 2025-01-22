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

// Function to retrieve and send the chunk data
void retrieveAndSendChunk(int rank, const string &chunkId)
{
    // Construct the file name from chunkId
    string fileName = "chunks/node_" + to_string(rank) + "_" + chunkId + ".bin";
    ifstream chunkFile(fileName.c_str(), ios::in | ios::binary);
    cout<<fileName<<endl;
    if (chunkFile.is_open())
    {
        // Read the chunk data
        char chunkDataBuffer[CHUNK_SIZE + 1];
        chunkFile.read(chunkDataBuffer, CHUNK_SIZE);

        // Ensure null-termination
        chunkDataBuffer[chunkFile.gcount()] = '\0';

        // Send the chunk data back to Rank 0
        MPI_Send(chunkDataBuffer, CHUNK_SIZE + 1, MPI_CHAR, 0, 0, MPI_COMM_WORLD);
        cout<<chunkDataBuffer<<endl;
        cout << "Node " << rank << " sending chunk: " << chunkId << " to Rank 0." << endl;
    }
    else
    {
        cerr << "Node " << rank << " could not open file for chunk: " << chunkId << endl;
    }
}
// Function to get the file size
size_t getFileSize(const string &filePath)
{
    struct stat fileStat;
    if (stat(filePath.c_str(), &fileStat) == 0)
    {
        return fileStat.st_size;
    }
    return 0;
}

// Function to read the file and partition it into chunks
vector<string> partitionFile(const string &filePath)
{
    ifstream file(filePath, ios::binary);
    vector<string> chunks;

    if (!file)
    {
        cerr << "Error: File not found or cannot be opened!" << endl;
        return chunks;
    }

    char buffer[CHUNK_SIZE];
    while (file.read(buffer, CHUNK_SIZE) || file.gcount() > 0)
    {
        chunks.push_back(string(buffer, file.gcount()));
    }

    file.close();
    return chunks;
}

// Function to distribute chunks across nodes with replication
map<string, vector<int>> distributeChunks(const vector<string> &chunks, int numNodes, const string &fileName)
{
    map<string, vector<int>> metadata;

    for (size_t i = 0; i < chunks.size(); ++i)
    {
        vector<int> replicas;

        // Distribute replicas to distinct nodes using modulo arithmetic
        for (int r = 0; r < REPLICATION_FACTOR; ++r)
        {
            int node = (i + r) % numNodes + 1; // Nodes are from 1 to N-1
            replicas.push_back(node);
        }

        // Create unique chunk ID with filename prefix
        string chunkId = fileName + "_Chunk_" + to_string(i);
        metadata[chunkId] = replicas;
    }

    return metadata;
}

void storeChunk(const string &chunkId, const string &chunkData, int rank)
{
    string directory = "chunks";

    // Check if the directory exists, and create it if it doesn't
    struct stat info;
    if (stat(directory.c_str(), &info) != 0)
    {
        if (mkdir(directory.c_str(), 0777) != 0)
        {
            cerr << "Error: Could not create directory!" << endl;
            return;
        }
    }

    string fileName = directory + "/node_" + to_string(rank) + "_" + chunkId + ".bin";
    ofstream file(fileName, ios::binary);

    if (file)
    {
        file.write(chunkData.c_str(), chunkData.size());
        file.close();
        cout << "Rank " << rank << " stored " << chunkId << " at " << fileName << "." << endl;
    }
    else
    {
        cerr << "Error: Could not store chunk at Rank " << rank << endl;
    }
}

void printMetadata(const map<string, vector<int>> &metadata)
{
    cout << "Metadata (Chunk -> Nodes holding that chunk):" << endl;
    for (const auto &[chunkId, nodes] : metadata)
    {
        cout << chunkId << " -> ";
        for (size_t i = 0; i < nodes.size(); ++i)
        {
            cout << "Node " << nodes[i];
            if (i != nodes.size() - 1)
            {
                cout << ", ";
            }
        }
        cout << endl;
    }
}
void retrieveFile(const string &fileName, const map<string, vector<int>> &fileChunks, int rank)
{
    if (rank != 0)
    {
        cout << "other ";
        cerr << "Error: Only Rank 0 can retrieve files." << endl;
        return;
    }
    vector<string> reassembledChunks(fileChunks.size(), "");

    cout << "Rank 0: Retrieving file " << endl;
    // Loop through each chunk in the file
    for (const auto &[chunkId, nodes] : fileChunks)
    {
        bool chunkRetrieved = false;
        // Try retrieving the chunk from each replica node
        for (const int &targetNode : nodes)
        {
            // Sending retrieval request to the target node
            MPI_Send("retrieve", strlen("retrieve") + 1, MPI_CHAR, targetNode, 0, MPI_COMM_WORLD);
            MPI_Send(chunkId.c_str(), chunkId.size() + 1, MPI_CHAR, targetNode, 0, MPI_COMM_WORLD);
            char chunkData[CHUNK_SIZE + 1] = {0}; // Ensure buffer is large enough
            MPI_Status status;
            MPI_Recv(chunkData, CHUNK_SIZE + 1, MPI_CHAR, targetNode, 0, MPI_COMM_WORLD, &status);

            // Log chunk data received from the node
            cout << "Rank 0: Received chunk data from Node " << targetNode << " for " << chunkId << ": " << chunkData << endl;

            // Validate and check if chunk data was received
            if (strlen(chunkData) > 0)
            {
                // Extract the chunk index from chunkId
                size_t underscorePos = chunkId.find_last_of('_');
                if (underscorePos != string::npos)
                {
                    int chunkIndex = stoi(chunkId.substr(underscorePos + 1));
                    if (chunkIndex < reassembledChunks.size())
                    {
                        reassembledChunks[chunkIndex] = string(chunkData);
                        chunkRetrieved = true;
                        break; // Stop trying other nodes once the chunk is retrieved
                    }
                }
            }
        }

        // If a chunk could not be retrieved, return an error and exit
        if (!chunkRetrieved)
        {
            cerr << "Error: Could not retrieve chunk \"" << chunkId << "\" from any replica." << endl;
            return;
        }
    }

    // Combine the chunks into the original file content
    string fileContent;
    for (const auto &chunk : reassembledChunks)
    {
        if (chunk.empty())
        {
            cerr << "Error: Missing chunk during reassembly." << endl;
            return;
        }
        fileContent += chunk;
    }

    // Display the reassembled file content
    cout << "Rank 0: Successfully retrieved and reassembled the file \"" << fileName << "\"." << endl;
    cout << "File Content:\n"
         << fileContent << endl;
}

int main(int argc, char *argv[])
{
    MPI_Init(&argc, &argv);

    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    if (size < 4)
    {
        if (rank == 0)
        {
            cerr << "Please run the program with at least 4 processes." << endl;
        }
        MPI_Finalize();
        return 0;
    }

    if (rank == 0)
    {
        // Rank 0 handles the file upload command in a loop
        map<string, map<string, vector<int>>> mdata;
        while (true)
        {
            string command;
            cout << "Enter command: ";
            getline(cin, command);

            if (command == "exit")
            {
                for (int i = 1; i < size; ++i)
                {
                    MPI_Send("exit", 5, MPI_CHAR, i, 0, MPI_COMM_WORLD);
                }
                cout << "Exiting program..." << endl;
                break;
            }

            if (command.rfind("upload", 0) == 0)
            {
                string filePath = command.substr(7);                                 // Extract the filename after "upload "
                string fileName = filePath.substr(filePath.find_last_of("/\\") + 1); // Extract filename from path

                if (filePath.empty())
                {
                    cerr << "Error: No filename provided!" << endl;
                    continue;
                }

                size_t fileSize = getFileSize(filePath);
                if (fileSize == 0)
                {
                    cerr << "Error: File not found or empty!" << endl;
                    continue;
                }
                cout << "Rank 0: Reading file: " << filePath << ", Size: " << fileSize << " bytes." << endl;

                vector<string> chunks = partitionFile(filePath);
                if (chunks.empty())
                {
                    cerr << "Error: Could not partition file!" << endl;
                    continue;
                }
                cout << "Rank 0: Partitioned file into " << chunks.size() << " chunks." << endl;

                map<string, vector<int>> chunkMetadata = distributeChunks(chunks, size - 1, fileName);

                printMetadata(chunkMetadata);
                mdata[fileName] = chunkMetadata;
                for (const auto &[chunkId, replicas] : chunkMetadata)
                {
                    for (int node : replicas)
                    {
                        MPI_Send("upload", 8, MPI_CHAR, node, 0, MPI_COMM_WORLD);
                        MPI_Send(chunkId.c_str(), chunkId.size() + 1, MPI_CHAR, node, 0, MPI_COMM_WORLD);
                        MPI_Send(chunks[stoi(chunkId.substr(chunkId.find_last_of('_') + 1))].c_str(),
                                 chunks[stoi(chunkId.substr(chunkId.find_last_of('_') + 1))].size() + 1,
                                 MPI_CHAR, node, 0, MPI_COMM_WORLD);
                    }
                }

                cout << "Rank 0: Uploaded file \"" << filePath << "\" successfully." << endl;
            }
            else if (command.rfind("list_file", 0) == 0)
            {
                string fileName = command.substr(10); // Extract the filename after "list_file "

                if (fileName.empty())
                {
                    cerr << "Error: No filename provided for listing!" << endl;
                    continue;
                }

                // Check if the metadata exists for the file
                if (mdata.find(fileName) == mdata.end())
                {
                    cerr << "Error: No metadata found for file \"" << fileName << "\"." << endl;
                    continue;
                }

                // Print metadata for the specified file
                const auto &chunkMetadata = mdata[fileName];
                cout << "Metadata for file \"" << fileName << "\":" << endl;
                printMetadata(chunkMetadata);
            }
            else if (command.rfind("retrieve", 0) == 0)
            {
                string fileName = command.substr(9);

                if (fileName.empty())
                {
                    cerr << "Error: No filename provided for retrieval!" << endl;
                    continue;
                }

                if (mdata.find(fileName) == mdata.end())
                {
                    cerr << "Error: No metadata found for file \"" << fileName << "\"." << endl;
                    continue;
                }

                retrieveFile(fileName, mdata[fileName], rank);
            }
            else
            {
                cerr << "Rank 0: Invalid command!" << endl;
            }
        }
    }
    else
    {

        //     while (true) {
        //     char commandBuffer[100];
        //     char chunkIdBuffer[100];
        //     char chunkDataBuffer[CHUNK_SIZE + 1];

        //     MPI_Status status;
        //     MPI_Recv(commandBuffer, 100, MPI_CHAR, 0, 0, MPI_COMM_WORLD, &status);
        //     string command(commandBuffer);
        //     if (command == "exit") {
        //                 cout << "Rank " << rank << ": Terminating..." << endl;
        //                 break;
        //             }
        //     else if (command == "retrieve") {
        //         // Receive the chunk ID to retrieve
        //         MPI_Recv(chunkIdBuffer, 100, MPI_CHAR, 0, 0, MPI_COMM_WORLD, &status);
        //         string chunkId(chunkIdBuffer);

        //         // Check if this node holds the requested chunk
        //         // Assuming that the chunk ID is formatted as the file name and index (example: fileName_Chunk_0)
        //         string fileName = chunkId.substr(0, chunkId.find_last_of('_'));  // Extract the file name

        //         // If this node holds the chunk, send it back to Rank 0
        //         {  // isThisNodeHoldingChunk should return true if the node holds the chunk
        //             MPI_Send(chunkDataBuffer, CHUNK_SIZE + 1, MPI_CHAR, 0, 0, MPI_COMM_WORLD);
        //             cout << "Node " << rank << " sending chunk: " << chunkId << " to Rank 0." << endl;
        //         }
        //     } else  {
        //         // Receive chunk ID and the chunk data for upload
        //         MPI_Recv(chunkIdBuffer, 100, MPI_CHAR, 0, 0, MPI_COMM_WORLD, &status);
        //         string chunkId(chunkIdBuffer);

        //         MPI_Recv(chunkDataBuffer, CHUNK_SIZE + 1, MPI_CHAR, 0, 0, MPI_COMM_WORLD, &status);
        //         string chunkData(chunkDataBuffer);

        //         // Store the chunk in this node's local storage
        //         storeChunk(chunkId, chunkData, rank);
        //         cout << "Node " << rank << " uploaded chunk: " << chunkId << endl;

        //     }
        // }
        while (true)
        {
            char commandBuffer[100];
            char chunkIdBuffer[100];
            char chunkDataBuffer[CHUNK_SIZE + 1];
            MPI_Status status;
            MPI_Recv(commandBuffer, 100, MPI_CHAR, 0, 0, MPI_COMM_WORLD, &status);
            string command(commandBuffer);
            cout << "command " << command << endl;
            if (command == "exit")
            {
                cout << "Rank " << rank << ": Terminating..." << endl;
                break;
            }
            else if (command == "upload")
            {

                MPI_Recv(chunkIdBuffer, 100, MPI_CHAR, 0, 0, MPI_COMM_WORLD, &status);
                string chunkId(chunkIdBuffer);

                MPI_Recv(chunkDataBuffer, CHUNK_SIZE + 1, MPI_CHAR, 0, 0, MPI_COMM_WORLD, &status);
                string chunkData(chunkDataBuffer);

                storeChunk(chunkId, chunkData, rank);
            }
            else if (command == "retrieve")
            {
                // Receive the chunk ID to retrieve
               
                MPI_Recv(chunkIdBuffer, 100, MPI_CHAR, 0, 0, MPI_COMM_WORLD, &status);
                string chunkId(chunkIdBuffer);
                retrieveAndSendChunk(rank, chunkId);
                cout << "Node " << rank << " sending chunk: " << chunkId << " to Rank 0." << endl;
            }
        }
    }
    MPI_Finalize();
    return 0;
}
