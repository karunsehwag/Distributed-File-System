#include <mpi.h>
#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <cstring>
#include <chrono>
#include <set>
#include <map>
#include <algorithm>
#include <sys/stat.h>
#include <sys/types.h>

#define CHUNK_SIZE 32
#define REPLICATION_FACTOR 3

using namespace std;

unordered_map<string, string> chunkDataMap;
unordered_map<int, vector<string>> nodeDataMap;
vector<bool> nodestatus(10, true);
set<int> activenodes;

// Function to retrieve and send the chunk data
void retrieveAndSendChunk(int rank, const string &chunkId)
{
    if (chunkDataMap.find(chunkId) != chunkDataMap.end())
    {
        const string &chunkData = chunkDataMap[chunkId];
        MPI_Send(chunkData.c_str(), CHUNK_SIZE + 1, MPI_CHAR, 0, 0, MPI_COMM_WORLD);
        cout << "Rank " << rank << ": Sent chunk " << chunkId << endl;
    }
    else
    {
        cerr << "Rank " << rank << ": Chunk not found: " << chunkId << endl;
        // Optionally, send an empty or error message
        string errorMessage = "Chunk not found";
        MPI_Send(errorMessage.c_str(), errorMessage.size() + 1, MPI_CHAR, 0, 0, MPI_COMM_WORLD);
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
        string chunkId = fileName + "_Chunk_" + to_string(i);

        // Distribute replicas to active nodes with the least chunks
        for (int r = 0; r < REPLICATION_FACTOR; ++r)
        {
            int leastLoadedNode = -1;
            size_t minLoad = SIZE_MAX;

            // Find the node with the least number of assigned chunks from active nodes
            for (int node : activenodes)
            {
                if (find(replicas.begin(), replicas.end(), node) == replicas.end()) // Ensure not assigning the same node again
                {
                    size_t currentLoad = nodeDataMap[node].size(); // Count chunks assigned to this node
                    if (currentLoad < minLoad)
                    {
                        leastLoadedNode = node;
                        minLoad = currentLoad;
                    }
                }
            }

            // Assign the chunk to the least loaded node
            if (leastLoadedNode != -1)
            {
                replicas.push_back(leastLoadedNode);
                nodeDataMap[leastLoadedNode].push_back(chunkId);
            }
        }

        metadata[chunkId] = replicas;
    }

    return metadata;
}

void storeChunk(const std::string &chunkId, const std::string &chunkData, int rank)
{
    // Store the chunk in memory
    chunkDataMap[chunkId] = chunkData;

    // Log the storage operation
    std::cout << "Rank " << rank << " stored " << chunkId << " in memory." << std::endl;
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
        cerr << "Error: Only Rank 0 can retrieve files." << endl;
        return;
    }

    vector<string> reassembledChunks(fileChunks.size(), "");
    cout << "Rank 0: Retrieving file " << fileName << endl;

    // Loop through each chunk in the file
    for (const auto &[chunkId, nodes] : fileChunks)
    {
        bool chunkRetrieved = false;
        int retryCount = 0;                     // Counter to track retry attempts
        MPI_Request requests[nodes.size() * 2]; // Requests for MPI_Isend and MPI_Irecv
        MPI_Status statuses[nodes.size()];      // To hold statuses for MPI_Irecv
        char chunkData[CHUNK_SIZE + 1] = {0};   // Buffer to hold the chunk data
        vector<int> completedNodes;             // To keep track of which nodes have responded

        // Send requests to all nodes
        for (size_t i = 0; i < nodes.size(); ++i)
        {
            int targetNode = nodes[i];
            MPI_Isend("retrieve", strlen("retrieve") + 1, MPI_CHAR, targetNode, 0, MPI_COMM_WORLD, &requests[i * 2]);
            MPI_Isend(chunkId.c_str(), chunkId.size() + 1, MPI_CHAR, targetNode, 0, MPI_COMM_WORLD, &requests[i * 2 + 1]);
        }

        // Non-blocking receive for each node
        for (size_t i = 0; i < nodes.size(); ++i)
        {
            int targetNode = nodes[i];
            MPI_Irecv(chunkData, CHUNK_SIZE + 1, MPI_CHAR, targetNode, 0, MPI_COMM_WORLD, &requests[i + nodes.size()]);
        }

        // Retry logic
        while (!chunkRetrieved && retryCount < 3)
        {
            // Check for completion of any of the receives
            for (size_t i = 0; i < nodes.size(); ++i)
            {
                int flag = 0;
                MPI_Test(&requests[i + nodes.size()], &flag, &statuses[i]);
                if (flag && find(completedNodes.begin(), completedNodes.end(), nodes[i]) == completedNodes.end()) // Check if this node has not already responded
                {
                    // Process the chunk data
                    if (strlen(chunkData) > 0)
                    {
                        cout << "Rank 0: Received chunk data from Node " << nodes[i] << " for " << chunkId << endl;

                        // Extract the chunk index from chunkId
                        size_t underscorePos = chunkId.find_last_of('_');
                        if (underscorePos != string::npos)
                        {
                            int chunkIndex = stoi(chunkId.substr(underscorePos + 1));
                            if (chunkIndex < reassembledChunks.size())
                            {
                                reassembledChunks[chunkIndex] = string(chunkData);
                                chunkRetrieved = true;
                                completedNodes.push_back(nodes[i]); // Mark node as completed
                                break;                              // Stop trying other nodes once the chunk is retrieved
                            }
                        }
                    }
                }
            }

            if (!chunkRetrieved)
            {
                retryCount++;
                if (retryCount < 3)
                {
                    cout << "Retrying (" << retryCount << "/3) to retrieve chunk: " << chunkId << endl;
                }
            }
        }

        // If a chunk could not be retrieved from any node after 3 retries, return an error and exit
        if (!chunkRetrieved)
        {
            cerr << "Error: Could not retrieve chunk \"" << chunkId << "\" from any replica after 3 attempts." << endl;
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

bool CHECK=true;
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
        for(int i=1;i<size;i++)
        {
            activenodes.insert(i);
        }
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
            else if (command.rfind("failover", 0) == 0)
            {
                int node = stoi(command.substr(8));
                if (node < 1 || node >= size)
                {
                    cerr << "Rank 0: Invalid node number!" << endl;
                    continue;
                }
                activenodes.erase(node);
                MPI_Send("failover", 8, MPI_CHAR, node, 0, MPI_COMM_WORLD);
            }
            else if (command.rfind("recover", 0) == 0)
            {
                int node = stoi(command.substr(8));
                if (node < 1 || node >= size)
                {
                    cerr << "Rank 0: Invalid node number!" << endl;
                    continue;
                }
                activenodes.insert(node);
                MPI_Send("recover", 8, MPI_CHAR, node, 0, MPI_COMM_WORLD);
            }
            else
            {
                cerr << "Rank 0: Invalid command!" << endl;
            }
        }
    }
    else
    {
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
            else if (command == "upload" && CHECK)
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
            }
            else if (command == "failover")
            {
                if (CHECK == false)
                {
                    cout << "Rank " << rank << ": Already in failover state." << endl;
                }
                else
                {
                    cout << "Rank " << rank << ": Node failed." << endl;
                    CHECK = false;
                }
            }
            else if (command == "recover")
            {
                if (CHECK == true)
                {
                    cout << "Rank " << rank << ": Already in recovered state." << endl;
                }
                else
                {
                    cout << "Rank " << rank << ": Node recovered." << endl;
                    CHECK = true;
                }
            }
        }
    }
    MPI_Finalize();
    return 0;
}
