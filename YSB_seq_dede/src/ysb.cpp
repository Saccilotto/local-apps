#include <regex>
#include <string>
#include <vector>
#include <iostream>
#include <map>
#include <unordered_map>
#include <chrono>
#include <cmath>
#include <fstream>

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
double time_taken = 0; // total time taken
std::chrono::time_point<std::chrono::high_resolution_clock> start_time; // start time

CampaignGenerator campaign_gen;

unordered_map<unsigned long, unsigned int> campaign_map = campaign_gen.getHashMap(); // hashmap
campaign_record *relational_table = campaign_gen.getRelationalTable(); // relational table

queue::blocking_queue<Item> queue1;
queue::blocking_queue<Item> queue2;
queue::blocking_queue<Item> queue3;

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

void source () {
    auto start_source_time = std::chrono::high_resolution_clock::now();
    start_time = start_source_time;

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
}

void filter () {
    while (1) {
        Item item = queue1.dequeue();
        if (queue1.empty() && time_taken >= EXEC_TIME) {
            break; 
        }
        if (item.event.event_type != event_type) {
            return;
        }
        queue2.enqueue(item);
    }
}

void join () {
    while (1) {
        Item item = queue2.dequeue();
        if (queue1.empty() && time_taken >= EXEC_TIME) {
            break; 
        }
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
}

auto aggregate () {
    while (1) {
        Item item = queue3.dequeue();
        if (queue1.empty() && time_taken >= EXEC_TIME) {
            break; 
        }
        aggregateFunctionINC(item.joined_event, item.result);
        logEvent(item.joined_event.cmp_id);
        auto possible_time_end = chrono::high_resolution_clock::now();
        time_taken = chrono::duration_cast<chrono::seconds>(possible_time_end - possible_time_end).count();
    }
}


int main(int argc, char* argv[]) {
    init_maps();  

    cout << "start" << endl;

    source();
    filter();
    join();
    aggregate();


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
