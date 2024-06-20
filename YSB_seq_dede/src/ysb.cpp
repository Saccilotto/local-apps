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

#define EXEC_TIME 10
#define N_CAMPAIGNS 10

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
    // char padding[28]; // padding
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

class Item {
public:
    struct event_t event;
    struct joined_event_t joined_event;
    struct result_t result;

    Item():    
        event(),
        joined_event(),
        result()
    {};

    ~Item(){};
};

// colocar structs dentro do item e deletar item_data (done)

// function for computing the final aggregates on tumbling windows (INCremental version)
void aggregateFunctionINC(const joined_event_t &event, result_t &result)
{
    result.count++;
    if (event.ts > result.lastUpdate) {
        result.lastUpdate = event.ts;
    }
}

// Main
// Topology: source -> filter -> joiner -> winAggregate -> sink
int main(int argc, char* argv[]) {
    // parse arguments from command line 
    // int option = 0;
    // int index = 0;

    unsigned int value=0; // compared with ad_type
    unsigned int event_type=0;  // compared with event_type

    vector<result_t> results;

    CampaignGenerator campaign_gen;
    unordered_map<unsigned long, unsigned int> map = campaign_gen.getHashMap(); // hashmap
    campaign_record *relational_table = campaign_gen.getRelationalTable(); // relational table

    Item item;

    cout << "start" << endl;
    auto start_source_time = high_resolution_clock::now();
    double source_time_taken = 0; //init time
    while(source_time_taken <= EXEC_TIME){

        // start stage 1
        unsigned long **ads_arrays = campaign_gen.getArrays(); // arrays of ads
        unsigned int adsPerCampaign = campaign_gen.getAdsCompaign(); // number of ads per campaign
        long generated_tuples; // tuples counter
        unsigned long current_time;

        item.event.user_id = 0; // not meaningful
        item.event.page_id = 0; // not meaningful
        item.event.ad_id = ads_arrays[(value % 100000) % (N_CAMPAIGNS * adsPerCampaign)][1]; // ad_id from the arrays of ads (random) between 0 and 99999 (N_CAMPAIGNS * adsPerCampaign)
        item.event.ad_type = (value % 100000) % 5; // 0, 1, 2, 3, 4 => banner, modal, sponsored-search, mail, mobile
        item.event.event_type = (value % 100000) % 3; // 0, 1, 2 => view, click, purchase
        item.event.ip = 1; // not meaningful    		
        value++;
        generated_tuples++;
        item.event.ts = round(source_time_taken) * pow(10, -9);

        if(item.event.event_type != event_type) {     
            //cout << "Event type is not view(0)" << endl;
            auto possible_source_end = high_resolution_clock::now();
            //source_time_taken = duration_cast<seconds>(possible_source_end - start_source_time).count();
            source_time_taken = duration_cast<seconds>(possible_source_end - start_source_time).count();
            continue;
        }
        
        // start stage 3

        // cout << "Stage3" << endl;
        auto it = map.find(item.event.ad_id);
        if (it == map.end()) {
            // cout << "Ad_id not found in map" << endl;
            auto possible_source_end = high_resolution_clock::now();
            //source_time_taken = duration_cast<seconds>(possible_source_end - start_source_time).count();
            source_time_taken = duration_cast<seconds>(possible_source_end - start_source_time).count();
            continue;
        } else {
            // cout << "Ad_id FOUND in map" << endl;
            campaign_record record = relational_table[(*it).second];
            //item_data joined_event_data;
            item.joined_event = joined_event_t(record.cmp_id, 0);

            item.joined_event.ts = item.event.ts;
            item.joined_event.ad_id = item.event.ad_id;
            item.joined_event.relational_ad_id = record.ad_id;
            item.joined_event.cmp_id = record.cmp_id;

            //item.items_storage.pop_back(); // remove the first element of alredy used event element
        }

        // start stage 4

        aggregateFunctionINC(item.joined_event, item.result);

        auto possible_source_end = high_resolution_clock::now();
        //source_time_taken = duration_cast<seconds>(possible_source_end - start_source_time).count();
        source_time_taken = duration_cast<seconds>(possible_source_end - start_source_time).count();
           
        // cout << "Stage5" << endl;
        // Start stage 5
        results.push_back(item.result);
        cout << "source_time_taken: " << source_time_taken << endl;




    } 

    for (int i = 0; i < results.size(); i++) {
        cout << "wid: " << results[i].wid << " cmp_id: " << results[i].cmp_id << " lastUpdate: " <<  results[i].lastUpdate << " count: " <<  results[i].count << endl;
    }

    cout << "end" << endl;
    cout << " ================================================================ " << endl;
    cout << "source_time_taken: " << source_time_taken << endl;
    cout << "item results size (total items that reached end): " << results.size() << endl;
    cout << " ================================================================ " << endl;
    //TODO: adicionar tempo incial e final no output    

    // get item last count from result

    return 0;
}
