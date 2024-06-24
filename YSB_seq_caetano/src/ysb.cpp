/**************************************************************************************
 *  Copyright (c) 2019- Gabriele Mencagli
 *  
 *  This file is part of StreamBenchmarks.
 *  
 *  StreamBenchmarks is free software dual licensed under the GNU LGPL or MIT License.
 *  You can redistribute it and/or modify it under the terms of the
 *    * GNU Lesser General Public License as published by
 *      the Free Software Foundation, either version 3 of the License, or
 *      (at your option) any later version
 *    OR
 *    * MIT License: https://github.com/ParaGroup/StreamBenchmarks/blob/master/LICENSE.MIT
 *  
 *  StreamBenchmarks is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Lesser General Public License for more details.
 *  You should have received a copy of the GNU Lesser General Public License and
 *  the MIT License along with WindFlow. If not, see <http://www.gnu.org/licenses/>
 *  and <http://opensource.org/licenses/MIT/>.
 **************************************************************************************
 */

#include <regex>
#include <string>
#include <vector>
#include <iostream>
#include <map>
#include <fstream>

#include "../includes/util/campaign_generator.hpp"
#include <chrono>
#include <math.h>

using namespace std;
using namespace chrono;

#define EXEC_TIME 3
#define N_CAMPAIGNS 10

unsigned int event_type = 0; // the event type to be filtered
unsigned int value = 0;


//std::map ads_per_campaing(N_CAMPAIGNS); //map dictionary of tuples

// Define the map to store campaign label as key and number of occurrences as value
std::map<int, int> campaign_events;
unsigned int total_generated_ads = 0; 
long generated_tuples = 0; 

void init_maps () {
    // Initialize the map with labels from 0 to N_CAMPAIGNS and occurrences set to 0
    for (int label = 0; label < N_CAMPAIGNS; ++label) {
        campaign_events[label] = 0;
    }
}

struct joined_event_t
{
    unsigned long ad_id; // advertisement id
    unsigned long relational_ad_id;
    unsigned long cmp_id; // campaign id
    uint64_t ts;

    // Constructor I
	joined_event_t():
				   ad_id(0),
				   relational_ad_id(0),
				   cmp_id(0),
				   ts(0) {}

    // Constructor II
    joined_event_t(unsigned long _cmp_id, uint64_t _id)
    {
    	cmp_id = _cmp_id;
    }
};

struct event_t
{
    unsigned long user_id; // user id
    unsigned long page_id; // page id
    unsigned long ad_id; // advertisement id
    unsigned int ad_type; // advertisement type (0, 1, 2, 3, 4) => ("banner", "modal", "sponsored-search", "mail", "mobile")
    unsigned int event_type; // event type (0, 1, 2) => ("view", "click", "purchase")
    unsigned int ip; // ip address
    uint64_t ts;
    //char padding[28]; // padding
};
struct result_t
{
    unsigned long wid; // id
    unsigned long cmp_id; // campaign id
    unsigned long lastUpdate; // MAX(TS)
    unsigned long count; // COUNT(*)

    // Constructor I
    result_t():
    		 wid(0),
    		 cmp_id(0),
    		 lastUpdate(0),
    		 count(0) {}

    // Constructor II
    result_t(unsigned long _cmp_id,
    		 int _wid):
    		 lastUpdate(0),
    		 count(0),
    		 cmp_id(_cmp_id),
    		 wid(_wid) {}
};

// function for computing the final aggregates on tumbling windows (INCremental version)
void aggregateFunctionINC(const joined_event_t &event, result_t &result)
{
    result.count++;

    //cout << "joined_event: " << event.ad_id << " " << event.relational_ad_id << " " << event.cmp_id << " " << event.ts << endl;

    if (event.ts > result.lastUpdate) {
        result.lastUpdate = event.ts;
    }
}

// Main
//Topology: source -> filter -> joiner -> winAggregate -> sink
int main(int argc, char* argv[]) {
    CampaignGenerator campaign_gen;

    unordered_map<unsigned long, unsigned int> map = campaign_gen.getHashMap(); // hashmap
    campaign_record *relational_table = campaign_gen.getRelationalTable(); // relational table
    
    init_maps();

    result_t result;

    cout << "start" << endl;
    double source_time_taken = 0;
    auto start_source_time = std::chrono::high_resolution_clock::now();
    while(source_time_taken <= EXEC_TIME){
        // start stage 1
        unsigned long **ads_arrays = campaign_gen.getArrays(); // arrays of ads
        unsigned int adsPerCampaings = campaign_gen.getAdsCompaign(); // number of total ads per campaigns

        total_generated_ads+=adsPerCampaings;

        unsigned long current_time;

        event_t event;
        event.user_id = 0; // not meaningful
        event.page_id = 0; // not meaningful
        event.ad_id = ads_arrays[(value % 100000) % (N_CAMPAIGNS * adsPerCampaings)][1];
        event.ad_type = (value % 100000) % 5;
        event.event_type = (value % 100000) % 3;
        event.ip = 1; // not meaningful    		

        event.ts = round(source_time_taken) * pow(10, -9);

        value++;
        generated_tuples++;

        // start stage 2
        // If the event type assigned as random between 0 and 2 is different from the event 
        // type(0 by default), skip the event
        if(event.event_type != event_type){ 
            continue;
        }
        
        // start stage 3
        joined_event_t *out;
        auto it = map.find(event.ad_id);
        // If the ad_id with the is not in the map, skip the event
        if (it == map.end()) { 
            continue;
        }
        else { // If the ad_id is in the map, create the joined event
            campaign_record record = relational_table[(*it).second];
            out = new joined_event_t(record.cmp_id, 0);
            out->ts = event.ts;
            out->ad_id = event.ad_id;
            out->relational_ad_id = record.ad_id;
            out->cmp_id = record.cmp_id; //retiravel

            // Incrementa o contador de eventos para a campanha específica
            campaign_events[out->cmp_id]++;
            // Incrementa o contador total de eventos
            //total_events++;
        }

        // start stage 4
        aggregateFunctionINC(*out, result);

        auto possible_source_end = chrono::high_resolution_clock::now();
        source_time_taken = chrono::duration_cast<chrono::seconds>(possible_source_end - start_source_time).count();

        // create Sink (Stage 5) (mainly for measuring latency and etc)
    }

    // Print the events_per_campaign map
    int total_ads = 0; 
    // cout << "Events per campaign:" << endl;
    // for (const auto &pair : campaign_events) {
    //     cout << "Campaign ID: " << pair.first << " - Number of Events: " << pair.second << endl;
    //     total_ads += pair.second;
    // }

    // Print the total number of events
    //cout << "Total number of events: " << total_events << endl;

    // cout << "Total final number of ads: " << total_ads << endl;

    // cout << "Total generated ads: " << total_generated_ads << endl;

    // cout << "Total numebr of generated tuples " << generated_tuples << endl;  

    // Write to file
    ofstream outfile("output.txt");

    cout << "\n";

    total_ads = 0;
    outfile << "Events per campaign:" << endl;
    for (const auto &pair : campaign_events) {
        outfile << "Campaign ID: " << pair.first << " - Number of Events: " << pair.second << endl;
        total_ads += pair.second;
    }
    
    outfile << "Total final number of ads: " << total_ads << endl;

    outfile << "Total generated ads: " << total_generated_ads << endl;

    outfile << "Total numebr of generated tuples " << generated_tuples << endl;  

    outfile << "Total time taken: " << source_time_taken << endl;

    cout << "end" << endl;
    
    return 0;
}
