#include <cassandra.h>
#include <iostream>
#include <string>
#include <sstream>
#include <chrono>
#include <fstream>
#include <unistd.h>
#include <random>
#include <thread>

void query_system_trace_duration(CassSession* session, const char* tracing_id,cass_int32_t *duration) ;
#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <algorithm>
#include <sys/stat.h>      // for file existence check
#include <cassandra.h>

// Struct to hold trace event data
struct TraceEvent {
    std::string session_id;
    std::string event_id;
    std::string activity;
    std::string source;
    cass_int32_t source_elapsed;
    std::string thread;
};


// Check if file exists
bool file_exists(const std::string& filename) {
    struct stat buffer;
    return (stat(filename.c_str(), &buffer) == 0);
}

// Your main function
void query_system_trace_Details(CassSession* session, const char* tracing_id,std::string filename ) {
    cass_int32_t duration = 0;
    query_system_trace_duration(session, tracing_id, &duration);
    std::cout << "Reading duration# " << duration << std::endl;

    if (duration == 0) {
        std::cout << "No tracing" << std::endl;
        return;
    }

    std::string query = "SELECT * FROM system_traces.events WHERE session_id = " + std::string(tracing_id) + ";";
    CassStatement* statement = cass_statement_new(query.c_str(), 0);
    CassFuture* future = cass_session_execute(session, statement);
    cass_future_wait(future);

    if (cass_future_error_code(future) == CASS_OK) {
        const CassResult* result = cass_future_get_result(future);
        CassIterator* rows = cass_iterator_from_result(result);

        std::vector<TraceEvent> events;

        while (cass_iterator_next(rows)) {
            const CassRow* row = cass_iterator_get_row(rows);
            TraceEvent event;

            // session_id
            CassUuid session_id;
            char session_id_str[CASS_UUID_STRING_LENGTH];
            if (cass_value_get_uuid(cass_row_get_column_by_name(row, "session_id"), &session_id) == CASS_OK) {
                cass_uuid_string(session_id, session_id_str);
                event.session_id = session_id_str;
            }

            // event_id
            CassUuid event_id;
            char event_id_str[CASS_UUID_STRING_LENGTH];
            if (cass_value_get_uuid(cass_row_get_column_by_name(row, "event_id"), &event_id) == CASS_OK) {
                cass_uuid_string(event_id, event_id_str);
                event.event_id = event_id_str;
            }

            // activity
            const char* activity;
            size_t activity_length;
            if (cass_value_get_string(cass_row_get_column_by_name(row, "activity"), &activity, &activity_length) == CASS_OK) {
                event.activity = std::string(activity, activity_length);
            }

            // source
            CassInet source;
            char source_str[CASS_INET_STRING_LENGTH];
            if (cass_value_get_inet(cass_row_get_column_by_name(row, "source"), &source) == CASS_OK) {
                cass_inet_string(source, source_str);
                event.source = source_str;
            }

            // source_elapsed
            cass_int32_t elapsed = 0;
            cass_value_get_int32(cass_row_get_column_by_name(row, "source_elapsed"), &elapsed);
            event.source_elapsed = elapsed;

            // thread
            const char* thread;
            size_t thread_length;
            if (cass_value_get_string(cass_row_get_column_by_name(row, "thread"), &thread, &thread_length) == CASS_OK) {
                event.thread = std::string(thread, thread_length);
            }

            events.push_back(event);
        }

        // Append to CSV
        bool exists = file_exists(filename);
        std::ofstream csv_file(filename, std::ios::app);

        if (!exists) {
            // Write header only once
            csv_file << "session_id,event_id,activity,source,source_elapsed,thread\n";
        }
        std::string tmp_session_id = " ";
        for (const auto& e : events) {
            csv_file << e.session_id << ","
                     << e.event_id << ","
                     << "\"" << e.activity << "\","
                     << e.source << ","
                     << e.source_elapsed << ","
                     << "\"" << e.thread << "\"\n";
                    tmp_session_id = e.session_id;
        }
        csv_file <<tmp_session_id << ","
                     << "final" << ","
                     << "\"" << "final" << "\","
                     << "final" << ","
                     << duration << ","
                     << "\"" << "final" << "\"\n";
        csv_file.close();
        std::cout << "CSV written to " << filename << "\n";

        cass_result_free(result);
        cass_iterator_free(rows);
    } else {
        const char* error_message;
        size_t error_message_length;
        cass_future_error_message(future, &error_message, &error_message_length);
        std::cerr << "Error querying system_trace: " << std::string(error_message, error_message_length) << std::endl;
    }

    cass_future_free(future);
    cass_statement_free(statement);
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

