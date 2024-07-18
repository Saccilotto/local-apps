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

#define EXEC_TIME 3
#define N_CAMPAIGNS 10

// #define NO_DETAILS
// #define NO_OUTPUT_FILE

using namespace std;
using namespace chrono;

unsigned int event_type = 0; // the event type to be filtered
unsigned int value = 0;

// Define the map to store campaign label as key and number of occurrences as value
map<int, int> campaign_events;
unsigned int total_generated_ads = 0; 
long generated_tuples = 0;  // total tuples counter

void logEvent(int campaignId) {
    campaign_events[campaignId]++;
}

void printToTerminal(bool printDetails, double source_time_taken) {
    if (printDetails) {
        for (const auto &pair : campaign_events) {
            cout << "Campaign ID: " << pair.first << " - Number of Events: " << pair.second << endl;
        }
    } else {
        cout << "Total number of generated tuples: " << generated_tuples << endl;
        cout << "Total generated ads: " << total_generated_ads << endl;
        cout << "Total time taken: " << source_time_taken << endl;
    }
}

void printToOutput(bool printDetails, double source_time_taken) {
    ofstream outfile("output.txt");
    if (outfile.is_open()) {
        if (printDetails) {
            for (const auto &pair : campaign_events) {
                outfile << "Campaign ID: " << pair.first << " - Number of Events: " << pair.second << endl;
            }
        } else {
            outfile << "Total number of generated tuples: " << generated_tuples << endl;
            outfile << "Total generated ads: " << total_generated_ads << endl;
        }
        outfile << "Total time taken: " << source_time_taken << endl;
        outfile.close();
    } else {
        cerr << "Unable to open file for writing" << endl;
    }
}

void endBench(bool printDetails, bool toTerminalOnly, double source_time_taken) {
    if (toTerminalOnly) {
        printToTerminal(printDetails, source_time_taken);
    } else {
        printToTerminal(printDetails, source_time_taken);
        printToOutput(printDetails, source_time_taken);
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

int main(int argc, char* argv[]) {
    CampaignGenerator campaign_gen;

    unordered_map<unsigned long, unsigned int> map = campaign_gen.getHashMap(); // hashmap
    campaign_record *relational_table = campaign_gen.getRelationalTable(); // relational table
    
    init_maps();

    result_t result;

    cout << "start" << endl;
    double source_time_taken = 0;
    auto start_source_time = high_resolution_clock::now();
    while (source_time_taken <= EXEC_TIME) {
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

        item.event.ts = round(source_time_taken) * pow(10, -9);

        value++;
        generated_tuples++;
    
        if (item.event.event_type != event_type) {     
            auto possible_source_end = high_resolution_clock::now();
            source_time_taken = duration_cast<seconds>(possible_source_end - start_source_time).count();
            continue;
        }

        auto it = map.find(item.event.ad_id);
        if (it == map.end()) {
            continue;
        } else {
            campaign_record record = relational_table[(*it).second];
            item.joined_event = joined_event_t(record.cmp_id, 0);
            item.joined_event.ts = item.event.ts;
            item.joined_event.ad_id = item.event.ad_id;
            item.joined_event.relational_ad_id = record.ad_id;
            item.joined_event.cmp_id = record.cmp_id;
        }

        aggregateFunctionINC(item.joined_event, item.result);

        logEvent(item.joined_event.cmp_id);

        auto possible_source_end = high_resolution_clock::now();
        source_time_taken = duration_cast<seconds>(possible_source_end - start_source_time).count();
    } 

    cout << "Processing completed. Generating final report..." << endl;

    // Choose whether to print details and where to print (to terminal or to file)
    bool printDetails = true; // default value to true
    bool toTerminalOnly = false; // default value to false, for printing both to terminal and file

#ifndef NO_OUTPUT_FILE
    toTerminalOnly = true;
#endif

#ifndef NO_DETAILS
    printDetails = false;
#endif

    endBench(printDetails, toTerminalOnly, source_time_taken);

    cout << "Final report generated." << endl;

    return 0;
}
