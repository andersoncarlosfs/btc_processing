/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.btc.controller.bolts;

import com.btc.controller.bolts.elasticsearch.BaseIndexerElasticSearchBolt;
import java.util.HashMap;
import java.util.Map;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

/**
 *
 * @author andersoncarlosfs
 */
public class IndexerBolt extends BaseIndexerElasticSearchBolt {

    @Override
    public String getTable() {
        return "rates";
    }

    @Override
    public Map<String, Object> toMap(Tuple tuple) {
        Map<String, Object> map = new HashMap<>();
        
        map.put("timestamp", tuple.getStringByField("timestamp"));
        map.put("rate", tuple.getDoubleByField("rate"));
        
        return map;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
    }

}
