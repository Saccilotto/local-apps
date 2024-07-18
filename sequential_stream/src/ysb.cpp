#include <regex>
#include <string>
#include <vector>
#include <iostream>
#include <map>
#include <unordered_map>
#include <chrono>
#include <cmath>
#include <fstream>
#include <thread>
#include <atomic>

#include "../includes/util/campaign_generator.hpp"
#include "queue.hpp"


#define EXEC_TIME 3
#define N_CAMPAIGNS 10

using namespace std;
using namespace chrono;

unsigned int event_type = 0; // the event type to be filtered
unsigned int value = 0;

// Define the map to store campaign label as key and number of occurrences as value
map<int, int> campaign_events;
unsigned int total_generated_ads = 0; 
long generated_tuples = 0;  // total tuples counter
atomic<double> time_taken{0.0}; // Use atomic for shared time variable
chrono::time_point<std::chrono::high_resolution_clock> start_time; // start time

CampaignGenerator campaign_gen;

unordered_map<unsigned long, unsigned int> campaign_map = campaign_gen.getHashMap(); // hashmap
campaign_record *relational_table = campaign_gen.getRelationalTable(); // relational table

struct joined_event_t {
    unsigned long ad_id; // advertisement id
    unsigned long relational_ad_id;
    unsigned long cmp_id; // campaign id
    uint64_t ts;

    joined_event_t() : ad_id(0), relational_ad_id(0), cmp_id(0), ts(0) {}

    joined_event_t(unsigned long _cmp_id, uint64_t _id)
        : ad_id(0), relational_ad_id(0), cmp_id(_cmp_id), ts(0) {}
};

struct event_t {
    unsigned long user_id; // user id
    unsigned long page_id; // page id
    unsigned long ad_id; // advertisement id
    unsigned int ad_type; // advertisement type (0, 1, 2, 3, 4) => ("banner", "modal", "sponsored-search", "mail", "mobile")
    unsigned int event_type; // event type (0, 1, 2) => ("view", "click", "purchase")
    unsigned int ip; // ip address
    uint64_t ts;
};

struct result_t {
    unsigned long wid; // id
    unsigned long cmp_id; // campaign id
    unsigned long lastUpdate; // MAX(TS)
    unsigned long count; // COUNT(*)

    result_t() : wid(0), cmp_id(0), lastUpdate(0), count(0) {}

    result_t(unsigned long _cmp_id, int _wid)
        : lastUpdate(0), count(0), cmp_id(_cmp_id), wid(_wid) {}
};

class Item {
public:
    event_t event;
    joined_event_t joined_event;
    result_t result;

    Item() : event(), joined_event(), result() {}
    ~Item() = default;
};

queues::blocking_queue<Item> queue1;
queues::blocking_queue<Item> queue2;
queues::blocking_queue<Item> queue3;

void logEvent(int campaignId) {
    campaign_events[campaignId]++;
}

void printToTerminal(bool printDetails, double time_taken) {
    if (printDetails) {
        for (const auto &pair : campaign_events) {
            cout << "Campaign ID: " << pair.first << " - Number of Events: " << pair.second << endl;
        }
    } 
    cout << "Total number of generated tuples: " << generated_tuples << endl;
    cout << "Total generated ads: " << total_generated_ads << endl;
    cout << "Total time taken: " << time_taken << endl;
}

void printToOutput(bool printDetails, double time_taken) {
    ofstream outfile("output.txt");
    if (outfile.is_open()) {
        if (printDetails) {
            for (const auto &pair : campaign_events) {
                outfile << "Campaign ID: " << pair.first << " - Number of Events: " << pair.second << endl;
            }
        } 
        outfile << "Total number of generated tuples: " << generated_tuples << endl;
        outfile << "Total generated ads: " << total_generated_ads << endl;
        outfile << "Total time taken: " << time_taken << endl;
        outfile.close();
    } else {
        cerr << "Unable to open file for writing" << endl;
    }
}

void endBench(bool printDetails, bool toTerminalOnly, double time_taken) {
    if (toTerminalOnly) {
        printToTerminal(printDetails, time_taken);
    } else {
        printToTerminal(printDetails, time_taken);
        printToOutput(printDetails, time_taken);
    }
}

void init_maps() {
    for (int label = 0; label < N_CAMPAIGNS; ++label) {
        campaign_events[label] = 0;
    }
}

void aggregateFunctionINC(const joined_event_t &event, result_t &result) {
    result.count++;
    if (event.ts > result.lastUpdate) {
        result.lastUpdate = event.ts;
    }
}

class Source {
public:   

    Source() = default;   

    void operator()() {
        start_time = std::chrono::high_resolution_clock::now();
        while (time_taken <= EXEC_TIME) {
            unsigned long **ads_arrays = campaign_gen.getArrays(); // arrays of ads
            unsigned int adsPerCampaigns = campaign_gen.getAdsCompaign(); // number of ads per campaign

            total_generated_ads += adsPerCampaigns;

            Item item;
            item.event.user_id = 0;
            item.event.page_id = 0;
            item.event.ad_id = ads_arrays[(value % 100000) % (N_CAMPAIGNS * adsPerCampaigns)][1];
            item.event.ad_type = (value % 100000) % 5;
            item.event.event_type = (value % 100000) % 3;
            item.event.ip = 1;

            item.event.ts = round(time_taken) * pow(10, -9);

            value++;
            generated_tuples++;
            queue1.enqueue(item);
        }
        return;
    };
};

class Filter {
public:   

    Filter() = default;   

    void operator()() {
        while (1) {
            if (time_taken >= EXEC_TIME) {
                break; 
            }
            Item item = queue1.dequeue();
            if (item.event.event_type != event_type) {
                return;
            }
            queue2.enqueue(item);
        }
        return;
    };
};

class Joiner {
public:   

    Joiner() = default;   

    void operator()() {
        while (1) {
            if (time_taken >= EXEC_TIME) {
                break; 
            }
            Item item = queue2.dequeue();
            auto it = campaign_map.find(item.event.ad_id);
            if (it == campaign_map.end()) {
                continue;
            } else {
                campaign_record record = relational_table[(*it).second];
                item.joined_event = joined_event_t(record.cmp_id, 0);
                item.joined_event.ts = item.event.ts;
                item.joined_event.ad_id = item.event.ad_id;
                item.joined_event.relational_ad_id = record.ad_id;
                item.joined_event.cmp_id = record.cmp_id;
                queue3.enqueue(item);
            }
        }
        return;
    };
};


class Aggregator {
public:   

    Aggregator() = default;   

        void operator()() {
            while (1) {
            if (time_taken >= EXEC_TIME) {
                break;  
            }
            Item item = queue3.dequeue();
            aggregateFunctionINC(item.joined_event, item.result);
            logEvent(item.joined_event.cmp_id);
            auto current_time = std::chrono::high_resolution_clock::now();
            auto elapsed_time = std::chrono::duration_cast<std::chrono::seconds>(current_time - start_time).count();
            time_taken.store(elapsed_time); // Atomic store operation
            cout << "Time taken: " << time_taken << endl;
        }
        return;
    };
};

int main(int argc, char* argv[]) {
    init_maps();  

    cout << "start" << endl;
    
    // Start the processing
    Source sourcing;
    Filter filtering;
    Joiner joining;
    Aggregator aggregating;

    thread source(sourcing);
    thread filter(filtering);
    thread joiner (joining);
    thread aggregator(aggregating);   
    
    // Join all threads
    source.join();
    filter.join();
    joiner.join();
    aggregator.join();



    // // Flags to track completion of each thread
    // bool source_done = false;
    // bool filter_done = false;
    // bool joiner_done = false;
    // bool aggregator_done = false;

    // while (!(source_done && filter_done && joiner_done && aggregator_done)) {
    //     if (source.joinable() && !source_done) {
    //         source.join();
    //         source_done = true;
    //         cout << "Source thread joined." << endl;
    //     }
    //     if (filter.joinable() && !filter_done) {
    //         filter.join();
    //         filter_done = true;
    //         cout << "Filter thread joined." << endl;
    //     }
    //     if (joiner.joinable() && !joiner_done) {
    //         joiner.join();
    //         joiner_done = true;
    //         cout << "Joiner thread joined." << endl;
    //     }
    //     if (aggregator.joinable() && !aggregator_done) {
    //         aggregator.join();
    //         aggregator_done = true;
    //         cout << "Aggregator thread joined." << endl;
    //     }
    //     // Sleep for a short while to avoid high CPU usage in the loop
    //     this_thread::sleep_for(chrono::milliseconds(100));
    // } 

    cout << "Processing completed." << endl;

    // Choose whether to print details and where to print (to terminal or to file)
    bool printDetails = true; // default value to true
    bool toTerminalOnly = false; // default value to false, for printing both to terminal and file

    #ifdef NO_DETAILS
    printDetails = false;
    #endif

    #ifdef NO_OUTPUT_FILE
    toTerminalOnly = true;
    #endif

    // Debug output
    cout << "printDetails: " << printDetails << endl;
    cout << "toTerminalOnly: " << toTerminalOnly << endl;

    endBench(printDetails, toTerminalOnly, time_taken);

    cout << "End." << endl;
    return 0;
}
