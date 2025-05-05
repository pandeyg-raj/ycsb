#include "cassandra_insert.h"

int main(int argc,char  ** argv) {
    
    // Initialize the Cassandra driver
    CassCluster* cluster = cass_cluster_new();
    CassSession* session = cass_session_new();

    // Set contact points (Cassandra node IP address)
    cass_cluster_set_contact_points(cluster, "10.158.34.27");
    cass_cluster_set_whitelist_filtering(cluster, "10.158.34.27");

    // Connect to the Cassandra cluster
    CassFuture* connect_future = cass_session_connect(session, cluster);
    cass_future_wait(connect_future);

    if (cass_future_error_code(connect_future) == CASS_OK) {
        std::cout << "Connected to Cassandra cluster!" << std::endl;

        // Create keyspace and table
        create_keyspace_and_table(session);

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
