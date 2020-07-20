/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.btc.controller.bolts;

import java.util.Map;
import org.apache.storm.shade.org.apache.commons.lang.StringEscapeUtils;
import org.apache.storm.shade.org.json.simple.JSONObject;
import org.apache.storm.shade.org.json.simple.parser.JSONParser;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

/**
 *
 * @author ?
 */
public class ConverterBolt extends BaseRichBolt {
    
    /**
     *
     */
    private OutputCollector collector;
    
    /**
     * 
     */
    private String time;
    private Double euro;  

    /**
     *
     * @param declarer
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {        
        declarer.declareStream("transactions", new Fields("source"));        
    }

    /**
     * 
     * @param topoConf
     * @param context
     * @param collector 
     */
    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    /**
     * 
     * @param input 
     */
    @Override
    public void execute(Tuple input) {
        try {
            JSONObject object = (JSONObject) input.getValueByField("source");
            
            if (object.containsKey("rate")) {
                this.time = (String) object.get("timestamp");
                this.euro = (Double) object.get("rate_float");
            } else {
                if (object.containsKey("total_amount")) {
                    object.put("timestamp", this.time);
                    object.put("euro", ((Double) object.get("total_amount")) * this.euro);
                                        
                    this.collector.emit("transactions", new Values(object));
                } else {
                    System.out.println(input);
                }
            }
            
            collector.ack(input);
        } catch (Exception e) {
            collector.reportError(e);
            collector.fail(input);
        }
    }

}
