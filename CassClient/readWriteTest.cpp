#include "cassandra_insert.h"

std::ofstream outfileCacheHit;
std::ofstream outfileCacheMiss;
std::ofstream outfileCacheCombine;

int main(int argc,char** argv) {
  
    int totalElement = 1;
    bool isTracingOn = 0;
    float ob_size_mb = 0.0001;
    bool isECObject = 1;
    int isInsert = 1;
    int isRead = 0;
    int inserIndex = 0;

    // Initialize the Cassandra driver
    CassCluster* cluster = cass_cluster_new();
    cass_cluster_set_contact_points(cluster, "10.10.1.2");
    //cass_cluster_set_whitelist_filtering(cluster, "10.10.1.2");
    CassSession* session = cass_session_new();

    
    // Connect to the Cassandra cluster
    CassFuture* connect_future = cass_session_connect(session, cluster);
    cass_future_wait(connect_future);
    
    if (cass_future_error_code(connect_future) == CASS_OK) {
        std::cout << "Connected to Cassandra cluster!" << std::endl;

        if(isInsert)
        {
            // Create keyspace and table
            //create_keyspace_and_table(session);
            
            for(int i=inserIndex;i<totalElement;i++)
            {   
                std::string insert_object = create_large_object(ob_size_mb,i);
                if(isECObject) // for ec cassandra add '0' at the begining 
                {
                    char byte = '\x00';  // Byte to prepend
                    insert_object.insert(insert_object.begin(), byte);  // Insert at the beginning
                   

                }
                //insert_large_object(session, i, insert_object);
                update_value(session, i, insert_object);
                
            }

        
            std::cout<<" Object insertion complete\n";
            std::this_thread::sleep_for(std::chrono::seconds(5));
        }
        
        if(isRead)
        {
            std::string teaceID = "";
            int64_t clientTime = 0;
            int64_t serverTime = 0;

            for(int i=0;i<totalElement;i++)
            {
                std::string readResult = read_large_object_by_key(session, i,i,clientTime,serverTime,teaceID,isTracingOn);
                
                if(readResult.compare(create_large_object(ob_size_mb,i)) !=0) //std::to_string(i)+ "someobject") !=0)
                {
                    std::cout<<"NOT :" <<i<<" Time:" <<clientTime<<" string not equals, \nGot:"<<readResult.length()<<"."<<std::endl<<"Exp:"<<create_large_object(ob_size_mb,i).length()<<"."<<std::endl;    
                    return-1;
                }
                else
                {
                    std::cout<<"YES :"<<i<<" Time:" <<clientTime<<"tracing "<<teaceID<<std::endl;//" string equals, Got: "<<readResult<<" Expected: "<<std::to_string(i)+ "someobject"<<std::endl;
                }
        
            }
        }

    } else {
        // Handle connection error
        const char* error_message;
        size_t error_message_length;
        cass_future_error_message(connect_future, &error_message, &error_message_length);
        std::cerr << "Connection error: " << std::string(error_message, error_message_length) << std::endl;
    }

    // Clean up
    outfileCacheHit.close();
    outfileCacheMiss.close();
    outfileCacheCombine.close();
    cass_future_free(connect_future);
    cass_session_free(session);
    cass_cluster_free(cluster);

    return 0;
}
