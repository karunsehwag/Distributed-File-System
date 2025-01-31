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
#include <thread>
#include <atomic>
#include <chrono>
#include <map>
#include <vector>
#include <ctime>
#include <regex>
#include <mutex>
#define CHUNK_SIZE 32
#define REPLICATION_FACTOR 3

using namespace std;

unordered_map<string, string> chunkDataMap;
unordered_map<int, vector<string>> nodeDataMap;
vector<bool> nodestatus(10, true);
set<int> activenodes;
atomic<bool> CHECK(true);
atomic<bool> BEAT(true);
const int heartbeatInterval = 1;    // Heartbeat interval in seconds
map<int, bool> heartbeats;          // Stores heartbeat status of each rank
map<int, time_t> lastHeartbeatTime; // Stores last heartbeat time for each rank
mutex heartbeatMutex;
const int failoverTimeout = 3; // Timeout for failover (in seconds)
// Function to retrieve and send the chunk data
void retrieveAndSendChunk(int rank, const string &chunkId)
{
    if (chunkDataMap.find(chunkId) != chunkDataMap.end())
    {
        const string &chunkData = chunkDataMap[chunkId];
        MPI_Send(chunkData.c_str(), CHUNK_SIZE + 1, MPI_CHAR, 0, 0, MPI_COMM_WORLD);
      //  cout << "Rank " << rank << ": Sent chunk " << chunkId << chunkData <<endl;
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
    // std::cout << "Rank " << rank << " stored " << chunkId << " in memory." << std::endl;
}

void printMetadata(const map<string, vector<int>> &metadata)
{
    

    int count = 0; // Initialize count for chunk indexing

    for (const auto &[chunkId, nodes] : metadata)
    {
        // Print the chunk index (count)
        cout << count << " ";
        count++;

        // Counter for number of active nodes
        int activeCount = 0;
        vector<int> activeNodes;  // To store active nodes for later printing
        
        // Check each node to see if it is active
        for (size_t i = 0; i < nodes.size(); ++i)
        {
            if (activenodes.find(nodes[i]) != activenodes.end())
            {
                activeNodes.push_back(nodes[i]);  // Store active node for later printing
                activeCount++;  // Increment active node count
            }
        }
          // Print the number of replicas (active node count)
        cout << activeCount;;
        // Print the active nodes
        sort(activeNodes.begin(), activeNodes.end());
        for (size_t i = 0; i < activeNodes.size(); ++i)
        {
            cout << " "<<activeNodes[i];  // Print the active node
        }
        cout<<endl;

       
    }
}

string retrieveFile(const map<string, vector<int>> &fileChunks, int rank)
{ 
    if (rank != 0)
    {
        cerr << "Error: Only Rank 0 can retrieve files." << endl;
        return "";
    }

    vector<string> reassembledChunks(fileChunks.size(), "");
    string fileContent;

    // Loop through each chunk in the file
    for (const auto &[chunkId, nodes] : fileChunks)
    {
        bool chunkRetrieved = false;
        MPI_Request requests[nodes.size() * 2]; // Requests for MPI_Isend and MPI_Irecv
        MPI_Status statuses[nodes.size()];      // To hold statuses for MPI_Irecv
        char chunkData[CHUNK_SIZE + 1] = {0};   // Buffer to hold the chunk data
        vector<int> completedNodes;             // To keep track of which nodes have responded
        std::vector<int> updatedNodes;

        // Loop through all nodes and keep only those that are in the activeNodes list
        for (int node : nodes)
        {
            if (std::find(activenodes.begin(), activenodes.end(), node) != activenodes.end())
            {
                updatedNodes.push_back(node); // Add active node to updated list
            }
        }

        // Use updatedNodes instead of modifying nodes
        // Send requests to all active nodes
        for (size_t i = 0; i < updatedNodes.size(); ++i)
        {
            int targetNode = updatedNodes[i];
            MPI_Isend("retrieve", strlen("retrieve") + 1, MPI_CHAR, targetNode, 1, MPI_COMM_WORLD, &requests[i * 2]);
            MPI_Isend(chunkId.c_str(), chunkId.size() + 1, MPI_CHAR, targetNode, 0, MPI_COMM_WORLD, &requests[i * 2 + 1]);
        }

        // Non-blocking receive for each active node
        for (size_t i = 0; i < updatedNodes.size(); ++i)
        {
            int targetNode = updatedNodes[i];
            MPI_Irecv(chunkData, CHUNK_SIZE + 1, MPI_CHAR, targetNode, 0, MPI_COMM_WORLD, &requests[i + updatedNodes.size()]);
        }

        // Timeout check (1 second)
        auto startTime = chrono::steady_clock::now();
        while (!chunkRetrieved)
        {
            // Check for completion of any of the receives
            for (size_t i = 0; i < updatedNodes.size(); ++i)
            {
                int flag = 0;

                MPI_Test(&requests[i + updatedNodes.size()], &flag, &statuses[i]);

                if (flag && find(completedNodes.begin(), completedNodes.end(), updatedNodes[i]) == completedNodes.end()) // Check if this node has not already responded
                {    
                    // Process the chunk data
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
                             //   cout<<"Rank 0: Received chunk data from Node " << updatedNodes[i] << " for " << chunkId << endl;
                                completedNodes.push_back(updatedNodes[i]); // Mark node as completed
                                break;                                     // Stop trying other nodes once the chunk is retrieved
                            }
                        }
                    }
                }
            }

            // Timeout check
            auto currentTime = chrono::steady_clock::now();
            double elapsedTime = chrono::duration<double>(currentTime - startTime).count();
            if (elapsedTime >= 1.0)
            {
                for (size_t i = 0; i < updatedNodes.size(); ++i)
                {
                    MPI_Cancel(&requests[i + updatedNodes.size()]);
                    MPI_Request_free(&requests[i + updatedNodes.size()]);
                }
                cerr << "-1" << chunkId << endl;
                break;
            }
        }

        // If a chunk could not be retrieved from any node, return an error and exit
        if (!chunkRetrieved)
        {
           
            return "-1";
        }
    }

    // Combine the chunks into the original file content
    for (const auto &chunk : reassembledChunks)
    {
        if (chunk.empty())
        {
            return "-1";
        }
        fileContent += chunk;
    }

    return fileContent;
}

void searchword(const string &word, const map<string, vector<int>> &fileChunks, int rank)
{
    // Retrieve the file content based on the chunk and rank
    string fileContent = retrieveFile(fileChunks, rank);

    vector<int> offsets;
    string pattern = "\\b" + word + "\\b"; // Word boundary regex to match the exact word
    regex wordRegex(pattern);              // Create a regex object with word boundaries

    auto words_begin = sregex_iterator(fileContent.begin(), fileContent.end(), wordRegex);
    auto words_end = sregex_iterator();

    // Loop through all matches and store their positions
    for (sregex_iterator i = words_begin; i != words_end; ++i)
    {
        smatch match = *i;
        offsets.push_back(match.position());
    }

    // Output the offsets
    if (!offsets.empty())
    {
        cout <<offsets.size()<< endl;
        for (int offset : offsets)
        {
            cout << offset << " ";
        }
        cout << endl;
    }
    else
    {
        cout << "-1" << endl;
    }
}




void sendHeartbeat(int rank)
{
    if (rank != 0) // Only non-zero ranks send heartbeats
    {
        int heartbeatCount = 0;
        while (BEAT) // Keep sending heartbeats
        {
            string message = "heartbeat";
            MPI_Request request;

            // Send heartbeat
            MPI_Isend(message.c_str(), message.size() + 1, MPI_CHAR, 0, 2, MPI_COMM_WORLD, &request);
           // cout << "Rank " << rank << " sent heartbeat " << ++heartbeatCount << endl;

            // Wait for 1 second before sending the next heartbeat
            this_thread::sleep_for(chrono::seconds(1));
        }
     //   cout << "Rank " << rank << " stopped sending heartbeats." << endl;
    }
}

// Function to monitor heartbeats continuously
void monitorHeartbeats(int rank, int size)
{
    if (rank == 0) // Only rank 0 monitors heartbeats
    {
        char commandBuffer[100];
        MPI_Status status;

        while (BEAT) // Keep monitoring heartbeats
        {
            MPI_Request request;
            int flag = 0;

            MPI_Irecv(commandBuffer, 100, MPI_CHAR, MPI_ANY_SOURCE, 2, MPI_COMM_WORLD, &request); // Non-blocking receive
            
            while (!flag && BEAT)
            {
                MPI_Test(&request, &flag, &status); // Check if a message has been received
                this_thread::sleep_for(chrono::milliseconds(100)); // Prevent busy waiting
            }

            if (flag)
            {
                string command(commandBuffer);
                if (command == "heartbeat")
                {
                    int senderRank = status.MPI_SOURCE;

                    // Update the last heartbeat time for the sender
                    lock_guard<mutex> lock(heartbeatMutex);
                    lastHeartbeatTime[senderRank] = time(0);

                 //   cout << "Rank 0 received heartbeat from rank " << senderRank  << " at " << lastHeartbeatTime[senderRank] << endl;
                }
            }
        }
     //   cout << "Rank 0 stopped monitoring heartbeats." << endl;
    }
}

void failoverCheck(int size)
{
    while (true)
    {
        time_t currentTime = time(0);

        for (int i = 1; i < size; ++i)
        {
            lock_guard<mutex> lock(heartbeatMutex); // Lock to ensure thread-safe access to heartbeats map
            if (heartbeats[i] && difftime(currentTime, lastHeartbeatTime[i]) > failoverTimeout)
            {
                heartbeats[i] = false; // Mark node as failed
                activenodes.erase(i);  // Remove the node from the active nodes set
                cout << "Node " << i << " has failed to send heartbeat in the last " << failoverTimeout << " seconds." << "  " << heartbeats[i] << endl;
                // Implement failover action here (e.g., reassign tasks)
            }
        }

        this_thread::sleep_for(chrono::seconds(1)); // Check every second
    }
}
void rtrim(string &str)
{
    str.erase(str.find_last_not_of(" \t\n\r\f\v") + 1);
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
        for (int i = 1; i < size; i++)
        {
            activenodes.insert(i);
        }
        // Rank 0 handles the file upload command in a loop
        map<string, map<string, vector<int>>> mdata;
        // thread heartbeatMonitor(monitorHeartbeats, rank, size); // Start heartbeat monitoring
        // heartbeatMonitor.detach();
        //        //thread failoverThread(failoverCheck, size);             // Start failover checking thread
        //    failoverThread.detach();

        while (true)
        {
            string command;
          
            getline(cin, command);

            if (command == "exit")
            {
                BEAT = false;
                for (int i = 1; i < size; ++i)
                {
                    MPI_Send("exit", 5, MPI_CHAR, i, 1, MPI_COMM_WORLD);
                }
                // cout << "Exiting program..." << endl;
                break;
            }

            if (command.rfind("upload", 0) == 0)
            {
                
    // Extract everything after "upload "
    string restOfCommand = command.substr(7);
    
    // Split the rest into file name and file path
    size_t spacePos = restOfCommand.find(' ');
    
        string fileName = restOfCommand.substr(0, spacePos);            // Extract the file name
        string filePath = restOfCommand.substr(spacePos + 1);           // Extract the file path

        // // Now you have both the file name and file path
        // cout << "File Name: " << fileName << endl;
        // cout << "File Path: " << filePath << endl;

  
                if (filePath.empty())
                {
                    cerr << "-1" << endl;
                    continue;
                }
                if (activenodes.empty())
                {
                    cerr << "-1" << endl;
                    continue;
                }

                size_t fileSize = getFileSize(filePath);
                if (fileSize == 0)
                {
                    cerr << "-1" << endl;
                    continue;
                }
                // cout << "Rank 0: Reading file: " << filePath << ", Size: " << fileSize << " bytes." << endl;

                vector<string> chunks = partitionFile(filePath);
                if (chunks.empty())
                {
                    cerr << "-1" << endl;
                    continue;
                }
                // cout << "Rank 0: Partitioned file into " << chunks.size() << " chunks." << endl;

                map<string, vector<int>> chunkMetadata = distributeChunks(chunks, size - 1, fileName);

                // printMetadata(chunkMetadata);
                mdata[fileName] = chunkMetadata;
                for (const auto &[chunkId, replicas] : chunkMetadata)
                {
                    for (int node : replicas)
                    {
                        MPI_Send("upload", 8, MPI_CHAR, node, 1, MPI_COMM_WORLD);
                        MPI_Send(chunkId.c_str(), chunkId.size() + 1, MPI_CHAR, node, 0, MPI_COMM_WORLD);
                        MPI_Send(chunks[stoi(chunkId.substr(chunkId.find_last_of('_') + 1))].c_str(),
                                 chunks[stoi(chunkId.substr(chunkId.find_last_of('_') + 1))].size() + 1,
                                 MPI_CHAR, node, 0, MPI_COMM_WORLD);
                    }
                }

                cout << "1" << endl;
                 printMetadata(chunkMetadata);
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
              //  cout << "Metadata for file \"" << fileName << "\":" << endl;
                printMetadata(chunkMetadata);
            }
            else if (command.rfind("retrieve", 0) == 0)
            {
                string fileName = command.substr(9);

                if (fileName.empty())
                {
                    cerr << "-1" << endl;
                    continue;
                }

                if (mdata.find(fileName) == mdata.end())
                {
                    cerr << "-1" << endl;
                    continue;
                }
           
                string filecontent = retrieveFile(mdata[fileName], rank);
                
                cout << filecontent << endl;
            }
            else if (command.rfind("search", 0) == 0)
            {
                // Extract the substring after "searchword " (which is the file name + word)
                string searchCommand = command.substr(7);

                // Find the position of the first space to split the command into file name and word
                size_t spacePos = searchCommand.find(' ');

                if (spacePos == string::npos)
                {
                    cerr << "-1" << endl;
                    continue;
                }

                // Extract the file name and word from the command
                string fileName = searchCommand.substr(0, spacePos);
                string word = searchCommand.substr(spacePos + 1);

                if (fileName.empty())
                {
                    cerr << "-1" << endl;
                    continue;
                }

                if (mdata.find(fileName) == mdata.end())
                {
                    cerr << "-1" << endl;
                    continue;
                }

                // Now call searchword with the file chunks and rank, while passing the word to be searched
                searchword(word, mdata[fileName], rank);
            }
            else if (command.rfind("failover", 0) == 0)
            {
                try
                {
                    // Extract the node number from the command
                    int node = stoi(command.substr(8));

                    // Validate the node number
                    if (node < 1 || node >= size)
                    {
                        cerr << "-1" << endl;
                        continue;
                    }

                    // Handle failover (remove node from active set)
                    activenodes.erase(node);
                    MPI_Send("failover", 9, MPI_CHAR, node, 1, MPI_COMM_WORLD);
                    cout << "1" << endl;
                }
                catch (const std::invalid_argument &e)
                {
                    cerr << "-1" << endl;
                }
                catch (const std::out_of_range &e)
                {
                    cerr << "-1" << endl;
                }
            }
            else if (command.rfind("recover", 0) == 0)
            {
                try
                {
                    int node = stoi(command.substr(8));
                    if (node < 1 || node >= size)
                    {
                        cerr << "-1" << endl;
                        continue;
                    }

                    activenodes.insert(node); // Remove the node from the active nodes set
                    MPI_Send("recover", 8, MPI_CHAR, node, 1, MPI_COMM_WORLD);
                     cout << "1" << endl;
                    // Further logic for failover/recover based on node value
                }
                catch (const std::invalid_argument &e) // Handle case where non-numeric characters are passed
                {
                    cerr << "-1" << endl;
                }
                catch (const std::out_of_range &e) // Handle case where the number is out of range for an int
                {
                    cerr << "-1" << endl;
                }
            }
            else
            {
                cerr << "-1" << endl;
            }
        }

    }
    else
    {
        thread heartbeatSender(sendHeartbeat, rank); // Start heartbeat sending
        heartbeatSender.detach();
        while (true)
        {
            char commandBuffer[100];
            char chunkIdBuffer[100];
            char chunkDataBuffer[CHUNK_SIZE + 1];
            MPI_Status status;
            MPI_Recv(commandBuffer, 100, MPI_CHAR, 0, 1, MPI_COMM_WORLD, &status);
            string command(commandBuffer);
            if (command == "exit")
            {
                // cout << "Rank " << rank << ": Terminating..." << endl;
                CHECK = false;
                BEAT = false;
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
            else if (command == "retrieve" && CHECK)
            {
                // Receive the chunk ID to retrieve

                MPI_Recv(chunkIdBuffer, 100, MPI_CHAR, 0, 0, MPI_COMM_WORLD, &status);
                string chunkId(chunkIdBuffer);
                //cout << "Rank " << rank << ": Received request to retrieve chunk " << chunkId << endl;
                retrieveAndSendChunk(rank, chunkId);
            }
            else if (command == "failover")
            {
                if (CHECK == false)
                {
                  //  cout << "-1" << endl;
                }
                else
                { // Remove the node from the active nodes set
                  //  cout << "1" << endl;
                    CHECK = false;
                }
            }
            else if (command == "recover")
            {
                if (CHECK == true)
                {
                  //  cout << "-1" << endl;
                }
                else
                {
                    // cout << "1" << endl;
                    CHECK = true;
                }
            }

        }
    }
    MPI_Finalize();
    return 0;
}
