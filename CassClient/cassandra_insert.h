#include <cassandra.h>
#include <iostream>
#include <string>
#include <sstream>
#include <chrono>
#include <fstream>
#include <unistd.h>
#include <random>
#include <thread>

enum cacheRead {
    MISS,
    MIX,
    HIT
}; 

// Function to create a large object (1MB of data as a string)
std::string create_large_object(float size_mb,int seed) {
    
    //std::string value =  std::string((size_mb *1000 * 1000), 'R');  // size_mb MB of 'R's as a string
    //return value;
    
    const std::string charset =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz"
        "0123456789";

    const size_t bytesPerMB = 1000 * 1000;
    size_t totalLength = static_cast<size_t>(size_mb * bytesPerMB);

    std::mt19937 gen(seed);
    std::uniform_int_distribution<> dist(0, charset.size() - 1);

    std::string result;
    result.reserve(totalLength);
    for (size_t i = 0; i < totalLength; ++i) {
        result += charset[dist(gen)];
    }

    return result;
}

// Function to query the system_trace keyspace using tracing_id
void query_system_trace_Details(CassSession* session, const char* tracing_id,bool &IsReadFromSSTable) {
    std::string query = "SELECT activity FROM system_traces.events WHERE session_id = " + std::string(tracing_id) + ";";
    // Execute the query
    CassFuture* future = cass_session_execute(session, cass_statement_new(query.c_str(), 0));
    cass_future_wait(future);
    if (cass_future_error_code(future) == CASS_OK) {
        const CassResult* result = cass_future_get_result(future);
        CassIterator* rows = cass_iterator_from_result(result);
        while (cass_iterator_next(rows)) {
            const CassRow* row = cass_iterator_get_row(rows);
            // Extract desired columns from the row
            const char* activity;
            size_t activity_length;
            cass_value_get_string(cass_row_get_column_by_name(row, "activity"), &activity, &activity_length );
            // Print session_id or other columns
            //std::cout << "compare: " << std::string(activity, activity_length) << std::endl;
            if (std::string(activity, activity_length).compare(0,20,"Partition index found",0,20) ==0)
            {
                IsReadFromSSTable = true;
            }
            
            // You can also query other related tables like system_trace.events for detailed trace
        }

        cass_result_free(result);
        cass_iterator_free(rows);
    } else {
        const char* error_message;
        size_t error_message_length;
        cass_future_error_message(future, &error_message, &error_message_length);
        std::cerr << "Error querying system_trace: " << std::string(error_message, error_message_length) << std::endl;
    }

    cass_future_free(future);
}



// Function to query the system_trace keyspace using tracing_id
void query_system_trace_duration(CassSession* session, const char* tracing_id,cass_int32_t *duration) {
    std::string query = "SELECT duration FROM system_traces.sessions WHERE session_id = " + std::string(tracing_id) + ";";
    // Execute the query
    CassFuture* future = cass_session_execute(session, cass_statement_new(query.c_str(), 0));
    cass_future_wait(future);
    if (cass_future_error_code(future) == CASS_OK) {
        const CassResult* result = cass_future_get_result(future);
        CassIterator* rows = cass_iterator_from_result(result);
        while (cass_iterator_next(rows)) {
            const CassRow* row = cass_iterator_get_row(rows);
            // Extract desired columns from the row
            cass_value_get_int32(cass_row_get_column_by_name(row, "duration"), duration );
            
            // Print session_id or other columns
            //std::cout << "Server_Duration_us: " << duration << std::endl;
            

            // You can also query other related tables like system_trace.events for detailed trace
        }

        cass_result_free(result);
        cass_iterator_free(rows);
    } else {
        const char* error_message;
        size_t error_message_length;
        cass_future_error_message(future, &error_message, &error_message_length);
        std::cerr << "Error querying system_trace: " << std::string(error_message, error_message_length) << std::endl;
    }

    cass_future_free(future);
}


// Function to insert a large object into the Cassandra table
void insert_large_object(CassSession* session, int key, const std::string& data) {
    // Insert query
    const char* insert_query = "INSERT INTO raj.rajt (id, field0) VALUES (?, ?);";

    // Create a statement
    CassStatement* statement = cass_statement_new(insert_query, 2);

    // Bind the integer key as id
    cass_statement_bind_int32(statement, 0, key);

    // Bind the string data
    cass_statement_bind_string_n(statement, 1, &data[0],data.size());
    // Set the consistency level to ALL
    cass_statement_set_consistency(statement, CASS_CONSISTENCY_ALL);

    // Execute the statement
    CassFuture* query_future = cass_session_execute(session, statement);
    cass_future_wait(query_future);

    if (cass_future_error_code(query_future) == CASS_OK) {
        std::cout << "Inserted object with key " << key << " and size " << data.size() << " bytes." << std::endl;
    } else {
        const char* error_message;
        size_t error_message_length;
        cass_future_error_message(query_future, &error_message, &error_message_length);
        std::cerr << "Insert error for key " << key << ": " << std::string(error_message, error_message_length) << std::endl;
        std::this_thread::sleep_for( std::chrono::milliseconds( 5000 ) );
        std::cout<<"retrying after 5 sec"<<std::endl;
        insert_large_object( session,  key, data);
    }

    cass_statement_free(statement);
    cass_future_free(query_future);
}




// Function to query the system_trace keyspace using tracing_id
void query_system_trace_Details_IsCacheHit(CassSession* session, const char* tracing_id,enum cacheRead& IsReadFromRowCache) {
    std::string query = "SELECT activity FROM system_traces.events WHERE session_id = " + std::string(tracing_id) + ";";
    // Execute the query
    CassFuture* future = cass_session_execute(session, cass_statement_new(query.c_str(), 0));
    cass_future_wait(future);
    if (cass_future_error_code(future) == CASS_OK) {
        const CassResult* result = cass_future_get_result(future);
        CassIterator* rows = cass_iterator_from_result(result);
        IsReadFromRowCache = MISS;
        bool cache_hit = false;
        bool cache_miss = false;
        while (cass_iterator_next(rows)) {
            const CassRow* row = cass_iterator_get_row(rows);
            // Extract desired columns from the row
            const char* activity;
            size_t activity_length;
            cass_value_get_string(cass_row_get_column_by_name(row, "activity"), &activity, &activity_length );
            // Print session_id or other columns
            //std::cout << "compare: " << std::string(activity, activity_length) << std::endl;
            if (std::string(activity, activity_length).compare(0,12,"Row cache hit",0,12) ==0)
            {
                cache_hit = true;
            }
            if (std::string(activity, activity_length).compare(0,12,"Row cache mis",0,12) ==0)
            {
                cache_miss = true;
            }
            // You can also query other related tables like system_trace.events for detailed trace
        }
        if(cache_miss)  
        {
            if(cache_hit) // mix
            {
                IsReadFromRowCache = MIX;
            }
            else // miss
            {
                IsReadFromRowCache = MISS;
            }
        }
        else    // hit
        {
            IsReadFromRowCache = HIT;
        }
       

        cass_result_free(result);
        cass_iterator_free(rows);
    } else {
        const char* error_message;
        size_t error_message_length;
        cass_future_error_message(future, &error_message, &error_message_length);
        std::cerr << "Error querying system_trace: " << std::string(error_message, error_message_length) << std::endl;
    }

    cass_future_free(future);
}



// Function to read and print a large object from the Cassandra table based on a given key
// also sets Total time taken based on client and server using "tracing feature"
std::string read_large_object_by_key(CassSession* session, int key,int index,int64_t& Client_Duration_us,int64_t& Server_Duration_us,std::string& traceID,bool isTracingOn ) {
    // Select query with a placeholder for the key
    const char* select_query = "SELECT id, field0 FROM raj.rajt WHERE id = ?;";

    // Create a statement
    CassStatement* statement = cass_statement_new(select_query, 1);

    // Set the consistency level to ALL
    cass_statement_set_consistency(statement, CASS_CONSISTENCY_QUORUM);

    // setting query tracing true if enabled
    if(isTracingOn)
    {
        cass_statement_set_tracing(statement, cass_true);
    }

    // Bind the key to the statement
    cass_statement_bind_int32(statement, 0, key);

    // timer start
    
    
    const char* data;
    size_t data_size;
    std::string finalresult ;;

    
    // Set timeout to 30-000 ms (30 seconds)
    cass_statement_set_request_timeout(statement, 30000);
    
    auto start = std::chrono::high_resolution_clock::now();
    
    // Execute the statement
    CassFuture* query_future = cass_session_execute(session, statement);

    // cass_future_wait(query_future);
    
    if (cass_future_error_code(query_future) == CASS_OK) 
    {

        const CassResult* result = cass_future_get_result(query_future);
        // timer end
        auto elapsed = std::chrono::high_resolution_clock::now() - start;
        Client_Duration_us = std::chrono::duration_cast<std::chrono::microseconds>(elapsed).count();
        //std::cout<<"Client_Duration_us: "<< Client_Duration_us<<std::endl;
        // ideally reading should dbe done here

        const CassRow* row = cass_result_first_row(result);

        if (row) {

            // Get the key (id)
            int32_t id;
            cass_value_get_int32(cass_row_get_column_by_name(row, "id"), &id);

            // Get the data (string)
            cass_value_get_string(cass_row_get_column_by_name(row, "field0"), &data, &data_size);
            finalresult =  std::string(data, data_size);
            //std::cout << "Read object with key " << id << " and size " << data_size << " bytes " << std::endl;
            

        } else {
            std::cout << "No object found with key " << key << "." << std::endl;
        }
        cass_result_free(result);
        
    } else {
        const char* error_message;
        size_t error_message_length;
        cass_future_error_message(query_future, &error_message, &error_message_length);
        std::cerr << "Read error: " << std::string(error_message, error_message_length) << std::endl;
        
    }
    // get the server duration from tracing details, only valid if tracing on 

    if(isTracingOn)
    {
        CassUuid tracing_id;
        if (cass_future_tracing_id(query_future, &tracing_id) == CASS_OK) 
        {
            char  outputt[CASS_UUID_STRING_LENGTH] = {0} ;
            cass_uuid_string (tracing_id, outputt );

            traceID = outputt; 
            //std::cout<<"tracingID is "<<outputt<<std::endl;
            // Now you can query the system_trace keyspace with the tracing_id
            cass_int32_t duration = 0;
            bool IsReadFromSSTable=false;
            
            retryDuration:
            query_system_trace_duration(session, outputt,&duration);
            if(duration == 0)
            {
                std::cout<<"duration is zero trying again\n";
                std::this_thread::sleep_for( std::chrono::milliseconds( 500 ) );
                goto retryDuration;
            }
            Server_Duration_us = duration ;

        } else {
        /* Handle error. If this happens then either a request error occurred or the
        * request type for the future does not support tracing.
        */
        }

    }
    
    
    cass_statement_free(statement);
    cass_future_free(query_future);

    return finalresult;

}

// Function to create the keyspace and table
void create_keyspace_and_table(CassSession* session) {
    const char* create_keyspace_query =
        "CREATE KEYSPACE IF NOT EXISTS raj WITH replication = {"
        "'class': 'SimpleStrategy', 'replication_factor': '5' }"
        "AND DURABLE_WRITES = false;";

    const char* create_table_query =
        "CREATE TABLE IF NOT EXISTS raj.rajt ("
        "id int,"
        "field0 text,"
        "PRIMARY KEY (id))" 
        "WITH caching = { 'keys' : 'NONE', 'rows_per_partition' : '1' } AND compression = { 'enabled' : false } AND read_repair='NONE';";

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


// Function to update a value for a specific integer key in a table
void update_value(CassSession* session, int key, const std::string& new_value) {
    const char* query = "UPDATE raj.rajt SET field0 = ? WHERE id = ?";
    
    // Bind the parameters
    CassStatement* statement = cass_statement_new(query,2);
      // Bind the string data
    cass_statement_bind_string_n(statement, 0, &new_value[0],new_value.size());

    cass_statement_bind_int32(statement, 1, key);  // Bind key as integer

    std::cout<< "value "<<new_value.c_str();
    // Execute the statement
    CassFuture* result_future = cass_session_execute(session, statement);
    cass_future_wait(result_future);

    if (cass_future_error_code(result_future) == CASS_OK) {
        std::cout << "Updated object with key " << key << " and size " << new_value.size() << " bytes." << std::endl;
    
    }
    else
    {
        const char* message;
        size_t message_length;
        cass_future_error_message(result_future, &message, &message_length);
        std::cerr << "Unable to run query: " << std::string(message, message_length) << std::endl;
        sleep(5);
        std::cout<<"retrying after 5 sec"<<std::endl;
        update_value( session, key,  new_value);
    }
    // Cleanup
    cass_future_free(result_future);
    cass_statement_free(statement);
}

