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

#include "../includes/util/campaign_generator.hpp"
#include <chrono>
#include <math.h>

#define EXEC_TIME 3
#define N_CAMPAIGNS 10

std::map ads_per_campaing(N_CAMPAIGNS); //map dictionary

using namespace std;
using namespace chrono;



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

    cout << "joined_event: " << event.ad_id << " " << event.relational_ad_id << " " << event.cmp_id << " " << event.ts << endl;

    if (event.ts > result.lastUpdate) {
        result.lastUpdate = event.ts;
    }
}

// Main
//Topology: source -> filter -> joiner -> winAggregate -> sink
int main(int argc, char* argv[]) {
    /// parse arguments from command line
    int option = 0;
    int index = 0;
    unsigned int value=0;
    
    CampaignGenerator campaign_gen;
    //unordered_map<unsigned long, unsigned int> map; // hashmap

    unordered_map<unsigned long, unsigned int> map = campaign_gen.getHashMap(); // hashmap
    campaign_record *relational_table = campaign_gen.getRelationalTable(); // relational table

    // *relational_table;
    unsigned int event_type=0;
    //joined_event_t joined_event;
    result_t result;
    cout << "start" << endl;
    auto start_source_time = std::chrono::high_resolution_clock::now();
    double source_time_taken = 0;

    

    while(source_time_taken <= EXEC_TIME){
        

        // start stage 1
        // cout << "stage1 "   << endl;
        unsigned long **ads_arrays = campaign_gen.getArrays(); // arrays of ads
        unsigned int adsPerCampaign = campaign_gen.getAdsCompaign(); // number of ads per campaign
        long generated_tuples; // tuples counter
        unsigned long current_time;
        event_t event;
        event.user_id = 0; // not meaningful
        event.page_id = 0; // not meaningful
        event.ad_id = ads_arrays[(value % 100000) % (N_CAMPAIGNS * adsPerCampaign)][1];
        event.ad_type = (value % 100000) % 5;
        event.event_type = (value % 100000) % 3;
        event.ip = 1; // not meaningful    		
        value++;
        generated_tuples++;
        event.ts = round(source_time_taken) * pow(10, -9);
        // start stage 2
        // cout << "Stage 2" << endl;
        if(event.event_type != event_type){
            continue;
        }

        joined_event_t *out;
        // start stage 3
        // cout << "Stage 3" << endl;
        auto it = map.find(event.ad_id);
        if (it == map.end()) {
            continue;
        }
        else {
            campaign_record record = relational_table[(*it).second];
            out = new joined_event_t(record.cmp_id, 0);
            out->ts = event.ts;
            out->ad_id = event.ad_id;
            out->relational_ad_id = record.ad_id;
            out->cmp_id = record.cmp_id; //retiravel
        }

        // start stage 4
        // cout << "joined_event: " << joined_event.ad_id << " " << joined_event.relational_ad_id << " " << joined_event.cmp_id << " " << joined_event.ts << endl; 
        aggregateFunctionINC(*out, result);

        auto possible_source_end = chrono::high_resolution_clock::now();
        source_time_taken = chrono::duration_cast<chrono::seconds>(possible_source_end - start_source_time).count();

        // create Sink (mainly for measuring latency and etc)
    }
    cout << "end" << endl;
    return 0;
}
