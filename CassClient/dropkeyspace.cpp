#include <cassandra.h>
#include <iostream>

void drop_keyspace(const char* keyspace) {
    // Create a cluster and connect to Cassandra
    CassCluster* cluster = cass_cluster_new();
    CassSession* session = cass_session_new();

    // Set contact point (change IP if necessary)
    cass_cluster_set_contact_points(cluster, "10.10.1.2");

    // Connect to cluster
    CassFuture* connect_future = cass_session_connect(session, cluster);
    if (cass_future_error_code(connect_future) != CASS_OK) {
        std::cerr << "Error connecting to Cassandra" << std::endl;
        cass_future_free(connect_future);
        cass_session_free(session);
        cass_cluster_free(cluster);
        return;
    }
    cass_future_free(connect_future);

    // Prepare DROP KEYSPACE query
    std::string query = "DROP KEYSPACE IF EXISTS " + std::string(keyspace);
    CassStatement* statement = cass_statement_new(query.c_str(), 0);

    // Execute query
    CassFuture* result_future = cass_session_execute(session, statement);
    if (cass_future_error_code(result_future) == CASS_OK) {
        std::cout << "Keyspace " << keyspace << " dropped successfully." << std::endl;
    } else {
        std::cerr << "Failed to drop keyspace."<< keyspace << std::endl;
    }

    // Clean up
    cass_future_free(result_future);
    cass_statement_free(statement);
    cass_session_close(session);
    cass_session_free(session);
    cass_cluster_free(cluster);
}

int main(int argc, char** argv) {
    const char* keyspace_name = argv[1];  // Change this to your keyspace
    if(argc !=2)
    {
        std::cout<<"usage: program [keyspacename]"<<std::endl;
        return -1;
    }
    drop_keyspace(keyspace_name);
    return 0;
}
