#include <cassandra.h>
#include <iostream>
#include <string>
#include <sstream>
#include <chrono>
#include <fstream>
#include <unistd.h>
#include <random>
#include <thread>


// Function to create the keyspace and table
void ycsb_create_keyspace_and_table(CassSession* session) {
    const char* create_keyspace_query =
        "CREATE KEYSPACE IF NOT EXISTS ycsb WITH replication = {"
        "'class': 'SimpleStrategy', 'replication_factor': '5' }"
        "AND DURABLE_WRITES = true;";

    const char* create_table_query =
        "CREATE TABLE IF NOT EXISTS ycsb.usertable ("
        "y_id varchar,"
        "field0 varchar,"
        "PRIMARY KEY (y_id))" 
        "WITH caching = { 'keys' : 'NONE', 'rows_per_partition' : 'none' } AND compression = { 'enabled' : false } AND read_repair='NONE';";

    // Create keyspace
    CassStatement* statement = cass_statement_new(create_keyspace_query, 0);
    CassFuture* future = cass_session_execute(session, statement);
    cass_future_wait(future);
    if (cass_future_error_code(future) != CASS_OK) {
        const char* error_message;
        size_t error_message_length;
        cass_future_error_message(future, &error_message, &error_message_length);
        std::cerr << "Failed to create keyspace: " << std::string(error_message, error_message_length) << std::endl;
    }
    std::cerr << "Create keyspace: Sucess waiting few second to populate" << std::endl;
    sleep(5);
   
    cass_statement_free(statement);
    cass_future_free(future);

    // Create table
    statement = cass_statement_new(create_table_query, 0);
    future = cass_session_execute(session, statement);
    cass_future_wait(future);
    if (cass_future_error_code(future) != CASS_OK) {
        const char* error_message;
        size_t error_message_length;
        cass_future_error_message(future, &error_message, &error_message_length);
        std::cerr << "Failed to create table: " << std::string(error_message, error_message_length) << std::endl;
    }
    std::cerr << "Create table: Sucess waiting few second to populate" << std::endl;
    sleep(10);
   
    cass_statement_free(statement);
    cass_future_free(future);
}

int main(int argc,char  ** argv) {
    
    // Initialize the Cassandra driver
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

        // Create keyspace and table
        ycsb_create_keyspace_and_table(session);

    } else {
        // Handle connection error
        const char* error_message;
        size_t error_message_length;
        cass_future_error_message(connect_future, &error_message, &error_message_length);
        std::cerr << "Connection error: " << std::string(error_message, error_message_length) << std::endl;
    }

    // Clean up

    cass_future_free(connect_future);
    cass_session_free(session);
    cass_cluster_free(cluster);

    return 0;
}
