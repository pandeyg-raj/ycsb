#include "cassandra_insert.h"
std::ofstream outfileCacheHit;
std::ofstream outfileCacheMiss;
//std::ofstream outfileCacheMix;
std::ofstream outfileCacheCombine;
std::ofstream ObjectMisMatch;
std::ofstream details;
//std::ofstream outfileWrite;


int main(int argc,char** argv) {
    
    bool isECObject = 1;
    float ob_size_mb = 1;       
    
   // int Total_starting_Data = 40000;     // total data in db before read/update
    int Total_starting_Data = 8000;
    int ShouldInsertStartingData = 1;
    int ShouldReadData = 0;
    int startingIndexInsertion = 0;


    bool isTracingOn = 0;
    int TotalOp = 1;     // total read+update operations
    
    int readPercent = 100;
    //int cacheTotal =  8192;  // total object that should be target for read aka read heavy objects
    int cacheTotal = 0 ;
    
    //int CacheReadPercent = 80 ;// percent of request of total read, target for cacheTotal object
    int CacheReadPercent = 0;

    int cacheRangeStart = 0;
    int cacheRangeEnd = cacheTotal; 
    int ssTableRangeStart = cacheTotal+1;
    int ssTableRangeEnd = Total_starting_Data-1;
    int is_update = 0;
    int missedRead = 0;
    std::string CacheHitFile = "Cache__Hit_CacheReadPercent" + std::to_string(CacheReadPercent)+ "readPercent" + std::to_string(readPercent) + "ObSize" + std::to_string((int)ob_size_mb) + "mb" + "_TotalOp" + std::to_string(TotalOp) + ".csv";
    std::string CacheMissFile = "Cache__Miss_CacheReadPercent" + std::to_string(CacheReadPercent)+ "readPercent" + std::to_string(readPercent) + "ObSize" + std::to_string((int)ob_size_mb) + "mb" + "_TotalOp" + std::to_string(TotalOp) + ".csv";
    //std::string CacheMixFile = "Cache__Mix_CacheReadPercent" + std::to_string(CacheReadPercent)+ "readPercent" + std::to_string(readPercent) + "ObSize" + std::to_string((int)ob_size_mb) + "mb" + "_TotalOp" + std::to_string(TotalOp) + ".csv";
    std::string CacheCombineFile = "Cache__Combine_CacheReadPercent" + std::to_string(CacheReadPercent)+ "readPercent" + std::to_string(readPercent) + "ObSize" + std::to_string((int)ob_size_mb) + "mb" + "_TotalOp" + std::to_string(TotalOp) + ".csv";
    //std::string WriteDetails = "Cache__WriteDeatails" + std::to_string(CacheReadPercent)+ "readPercent" + std::to_string(readPercent) + "ObSize" + std::to_string((int)ob_size_mb) + "mb" + "_TotalOp" + std::to_string(TotalOp) + ".csv";
    
    ObjectMisMatch.open("ObjectMisMatch.txt");
    details.open("Cache__details.txt");
    outfileCacheHit.open(CacheHitFile.c_str()); 
    outfileCacheHit<<"index,objectKey,colorr,client_lat_us,server_lat_us,traceID,"<<std::endl;
    outfileCacheMiss.open(CacheMissFile.c_str()); 
    outfileCacheMiss<<"index,objectKey,colorr,client_lat_us,server_lat_us,traceID,"<<std::endl;
    //outfileCacheMix.open(CacheMixFile.c_str()); 
    //outfileCacheMix<<"index,objectKey,colorr,client_lat_us,server_lat_us,traceID,"<<std::endl;
    outfileCacheCombine.open(CacheCombineFile.c_str());
    outfileCacheCombine<<"index,objectKey,colorr,client_lat_us,server_lat_us,traceID,"<<std::endl;
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

        
        // Insert large objects (1MB each)
        int TotalReadOP = 0;
        int TotalUpdate = 0;
        int64_t clientTime,serverTime;
        int64_t cacheHit = 0 ;
        int64_t cacheMiss = 0 ;
        //int64_t cacheMix = 0 ;
        enum cacheRead IsReadFromRowCache = MISS;
        std::string teaceID; 
        std::random_device dev;

        //std::mt19937 rng(dev());
        std::mt19937 rng(2935550409);

        std::uniform_int_distribution<std::mt19937::result_type> rangeFind(0,100); // 
        std::uniform_int_distribution<std::mt19937::result_type> cacheRan(cacheRangeStart,cacheRangeEnd);
        std::uniform_int_distribution<std::mt19937::result_type> ssTableRan(ssTableRangeStart,ssTableRangeEnd);
        std::uniform_int_distribution<std::mt19937::result_type> RanUpdate(0,ssTableRangeEnd);// ssTableRangeEnd is same Total_starting_Data

        
        if(ShouldInsertStartingData)
        {
            //outfileWrite.open(WriteDetails);
            //outfileWrite<<"write# , TimeTaken "<<std::endl;
            // Create keyspace and table
            create_keyspace_and_table(session);
            std::cout << "Keyspace and tablecreated!" << std::endl;

            for(int i=startingIndexInsertion;i<Total_starting_Data;i++)
            {
                std::string insert_object = create_large_object(ob_size_mb,i);   
                if(isECObject) // for ec cassandra add '0' at the begining 
                {
                    char byte = '\x00';  // Byte to prepend
                    insert_object.insert(insert_object.begin(), byte);  // Insert at the beginning
                }
                //auto start = std::chrono::high_resolution_clock::now();
                insert_large_object(session, i, insert_object);
                //auto elapsed = std::chrono::high_resolution_clock::now() - start;
                //outfileWrite<<i<<","<<std::chrono::duration_cast<std::chrono::microseconds>(elapsed).count() <<std::endl;

            }
            //outfileWrite.close();
        }


        if(!ShouldReadData)
        {
            return 0;
        }
        //std::this_thread::sleep_for( std::chrono::milliseconds( 120000 ) );
           
        for (int index = 0; index < TotalOp; index++) 
        {   
            if (rangeFind (rng) < readPercent)
            {
                is_update = 0;
            }
            else
            {
                is_update = 1;

            }
            if(is_update)
            {  
                int tmp = RanUpdate(rng);
                //std::string new_value = large_object+ std::to_string(tmp) + large_object+ std::to_string(tmp);
                //create_large_object(ob_size_mb);
                // update a random key

                //update_value(session, tmp , large_object+ std::to_string(tmp) + large_object+ std::to_string(tmp));  
                std::string insert_object = create_large_object(ob_size_mb,tmp);   
                if(isECObject) // for ec cassandra add '0' at the begining 
                {
                    char byte = '\x00';  // Byte to prepend
                    insert_object.insert(insert_object.begin(), byte);  // Insert at the beginning
                }
                
                insert_large_object(session, tmp, insert_object);
                TotalUpdate++;
            }
            else
            {
                TotalReadOP++;
                if (rangeFind (rng) < CacheReadPercent)
                {
                    // generate random key from cache
                    int obj_key;
                    do
                    {
                        obj_key = cacheRan(rng);
                    }while(obj_key > 4714010 && obj_key < 5000000);
                    
                    std::cout << index <<" Reading the request obj_key# " << obj_key << ":" << std::endl;
                    std::string readObject = read_large_object_by_key(session, obj_key,index,clientTime,serverTime,teaceID,isTracingOn);//dist6(rng),index);
                    
                    //make sure data is correct

                    if( readObject.compare( create_large_object(ob_size_mb,obj_key)) !=0)
                   { missedRead++;}
                    /*
                    {
                        std::cout<< "object mismatch\n";
                        ObjectMisMatch<<"Length of expected object "<<create_large_object(ob_size_mb,obj_key).size()<<std::endl;
                        ObjectMisMatch<<"Length of readObject object "<<readObject.size()<<std::endl;
                        ObjectMisMatch<<"Expected object "<<std::endl;
                        ObjectMisMatch<<"readObject object "<<std::endl;
                        ObjectMisMatch<<create_large_object(ob_size_mb,obj_key)<<std::endl<<std::endl;
                        ObjectMisMatch<<readObject<<std::endl;
                        ObjectMisMatch.close();
                        return -1;
                    }
                        */
                    // this information only available if tracing on 

                    if(isTracingOn )
                    {
                        query_system_trace_Details_IsCacheHit(session,teaceID.c_str(), IsReadFromRowCache);
                        
                        if(IsReadFromRowCache == HIT)  
                        {
                            cacheHit++;
                            std::cout<<"cache hit for key#"<<obj_key<<" traceID "<<teaceID<<std::endl;
                            outfileCacheHit<<index<<","<<obj_key<<",green,"<<clientTime<<","<<serverTime<<","<<teaceID<<",\n"; 
                            outfileCacheCombine<<index<<","<<obj_key<<",green,"<<clientTime<<","<<serverTime<<","<<teaceID<<",\n"; 
                        }
                        else if(IsReadFromRowCache == MISS)
                        { 
                            cacheMiss++;
                            std::cout<<"cache miss for key#"<<obj_key<<" traceID "<<teaceID<<std::endl;
                            outfileCacheMiss<<index<<","<<obj_key<<",red,"<<clientTime<<","<<serverTime<<","<<teaceID<<",\n";
                            outfileCacheCombine<<index<<","<<obj_key<<",red,"<<clientTime<<","<<serverTime<<","<<teaceID<<",\n";
                        }
                        else
                        { 
                            /* mix is miss
                            cacheMix++;
                            std::cout<<"cache mix for key#"<<obj_key<<" traceID "<<teaceID<<std::endl;
                            outfileCacheMix<<index<<","<<obj_key<<",red,"<<clientTime<<","<<serverTime<<","<<teaceID<<",\n";
                            outfileCacheCombine<<index<<","<<obj_key<<",red,"<<clientTime<<","<<serverTime<<","<<teaceID<<",\n";
                            */

                            cacheMiss++;
                            std::cout<<"cache miss for key#"<<obj_key<<" traceID "<<teaceID<<std::endl;
                            outfileCacheMiss<<index<<","<<obj_key<<",red,"<<clientTime<<","<<serverTime<<","<<teaceID<<",\n";
                            outfileCacheCombine<<index<<","<<obj_key<<",red,"<<clientTime<<","<<serverTime<<","<<teaceID<<",\n";
                        }
                    }
                    else
                    {
                        outfileCacheCombine<<index<<","<<obj_key<<",red,"<<clientTime<<","<<serverTime<<","<<teaceID<<",\n";
                    }
                }
                else
                {
                    // generate random key from sstable
                    int obj_key;
                    do
                    {
                        obj_key = ssTableRan(rng);
                    }while(obj_key > 4714010 && obj_key < 5000000);

                    
                    std::cout << index << " Reading the request obj_key# " << obj_key << ":" << std::endl;
                    std::string readObject = read_large_object_by_key(session, obj_key,index,clientTime,serverTime,teaceID,isTracingOn);//dist6(rng),index);
                    
                    if( readObject.compare( create_large_object(ob_size_mb,obj_key)) !=0)
                    { missedRead++;}
                    /*
                    {
                        std::cout<< "object mismatch sstable\n";
                        ObjectMisMatch<<"Length of expected object "<<create_large_object(ob_size_mb,obj_key).size()<<std::endl;
                        ObjectMisMatch<<"Length of readObject object "<<readObject.size()<<std::endl;
                        ObjectMisMatch<<"\n\nExpected object "<<std::endl;
                        ObjectMisMatch<<create_large_object(ob_size_mb,obj_key)<<std::endl<<std::endl;
                        ObjectMisMatch<<"readObject object "<<std::endl;
                        ObjectMisMatch<<readObject<<std::endl;
                        ObjectMisMatch.close();
                        return -1;
                    }
                        */
                    if(isTracingOn )
                    {
                        query_system_trace_Details_IsCacheHit(session,teaceID.c_str(), IsReadFromRowCache);
                    
                        if(IsReadFromRowCache == HIT)  
                        {
                            std::cout<<"cache hit for key#"<<obj_key<<" traceID "<<teaceID<<std::endl;
                            outfileCacheHit<<index<<","<<obj_key<<",green,"<<clientTime<<","<<serverTime<<","<<teaceID<<",\n";   
                            outfileCacheCombine<<index<<","<<obj_key<<",green,"<<clientTime<<","<<serverTime<<","<<teaceID<<",\n";
                            cacheHit++;
                        }
                        else if(IsReadFromRowCache == MISS)
                        {   
                            cacheMiss++;
                            std::cout<<"cache miss for key#"<<obj_key<<" traceID "<<teaceID<<std::endl;
                            outfileCacheMiss<<index<<","<<obj_key<<",red,"<<clientTime<<","<<serverTime<<","<<teaceID<<",\n";   
                            outfileCacheCombine<<index<<","<<obj_key<<",red,"<<clientTime<<","<<serverTime<<","<<teaceID<<",\n";
                            
                        }
                        else 
                        {   
                            cacheMiss++;
                            std::cout<<"cache miss for key#"<<obj_key<<" traceID "<<teaceID<<std::endl;
                            outfileCacheMiss<<index<<","<<obj_key<<",red,"<<clientTime<<","<<serverTime<<","<<teaceID<<",\n";   
                            outfileCacheCombine<<index<<","<<obj_key<<",red,"<<clientTime<<","<<serverTime<<","<<teaceID<<",\n";

                            /* mix is miss
                            cacheMix++;
                            std::cout<<"cache mix for key#"<<obj_key<<" traceID "<<std::endl;
                            outfileCacheMix<<index<<","<<obj_key<<",red,"<<clientTime<<","<<serverTime<<","<<teaceID<<",\n";   
                            outfileCacheCombine<<index<<","<<obj_key<<",red,"<<clientTime<<","<<serverTime<<","<<teaceID<<",\n";
                            */
                        }
                    }
                    else
                    {
                        outfileCacheCombine<<index<<","<<obj_key<<",red,"<<clientTime<<","<<serverTime<<","<<teaceID<<",\n";
                    }
                }
                
            }
        }

        details<<"Total reqs:"<<TotalOp<<std::endl;
        details<<"Total read:"<<TotalReadOP<<"  "<<((TotalReadOP*1.0)/TotalOp)*100<<" %"<<std::endl;
        details<<"Total update:"<<TotalUpdate<<"  "<<((TotalUpdate*1.0)/TotalOp)*100<<" %"<<std::endl;
        details<<"cache  hit:"<<cacheHit<<"  "<<((cacheHit*1.0)/TotalReadOP)*100<<" %"<<std::endl;
        details<<"cache Miss:"<<cacheMiss<<"  "<<((cacheMiss*1.0)/TotalReadOP)*100<<" %"<<std::endl;
        details<<"misread:"<< missedRead<<std::endl;


        std::cout<<"Total reqs:"<<TotalOp<<std::endl;
        std::cout<<"Total read:"<<TotalReadOP<<"  "<<((TotalReadOP*1.0)/TotalOp)*100<<" %"<<std::endl;
        std::cout<<"Total update:"<<TotalUpdate<<"  "<<((TotalUpdate*1.0)/TotalOp)*100<<" %"<<std::endl;
        std::cout<<"cache  hit:"<<cacheHit<<"  "<<((cacheHit*1.0)/TotalReadOP)*100<<" %"<<std::endl;
        std::cout<<"cache Miss:"<<cacheMiss<<"  "<<((cacheMiss*1.0)/TotalReadOP)*100<<" %"<<std::endl;
        std::cout<<"misRead:"<<missedRead<<std::endl;

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
    //outfileCacheMix.close();
    outfileCacheCombine.close();
    details.close();
    cass_future_free(connect_future);
    cass_session_free(session);
    cass_cluster_free(cluster);

    return 0;
}
