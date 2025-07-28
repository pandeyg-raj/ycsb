#include "extractTraceInfo.h"
std::ofstream outfileCacheHit;
std::ofstream outfileCacheMiss;
//std::ofstream outfileCacheMix;
std::ofstream outfileCacheCombine;
std::ofstream ObjectMisMatch;
std::ofstream details;
//std::ofstream outfileWrite;

int main(int argc,char** argv) {
    
    std::string TraceOutputfilename = "rep_trace_output95.csv";
    std::string TraceInputfilename = "rep_trace95.log";
    CassCluster* cluster = cass_cluster_new();
    CassSession* session = cass_session_new();

    // Set contact points (Cassandra node IP address)
    cass_cluster_set_contact_points(cluster, "10.10.1.2");
    cass_cluster_set_whitelist_filtering(cluster, "10.10.1.2");

    // Connect to the Cassandra cluster
    CassFuture* connect_future = cass_session_connect(session, cluster);
    cass_future_wait(connect_future);

    if (cass_future_error_code(connect_future) == CASS_OK) {
        std::cout << "Connected to Cassandra cluster!" << std::endl;  

        std::ifstream infile(TraceInputfilename); // Replace with your file name
        if (!infile) {
            std::cerr << "Failed to open the file.\n";
            return 1;
        }

        std::string line;
        int index = 1;
        while (std::getline(infile, line)) {
            const char* traceId = line.c_str();  // Convert std::string to const char*
            std::cout << "Read line: " <<index++ <<" "<< traceId << std::endl;   
            query_system_trace_Details(session,traceId,TraceOutputfilename);
        }
            infile.close();
            
    }
    else {
        // Handle connection error
        const char* error_message;
        size_t error_message_length;
        cass_future_error_message(connect_future, &error_message, &error_message_length);
        std::cerr << "Connection error: " << std::string(error_message, error_message_length) << std::endl;
    }

    // Clean up
    //outfileCacheHit.close();
    cass_future_free(connect_future);
    cass_session_free(session);
    cass_cluster_free(cluster);

    return 0;
}
