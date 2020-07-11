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
public class RaterBolt extends BaseRichBolt {

    /**
     *
     */
    private static final JSONParser PARSER = new JSONParser();

    /**
     *
     */
    private OutputCollector collector;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("timestamp", "rate"));
    }

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        try {
            //
            RaterBolt.PARSER.reset();

            //
            String string = StringEscapeUtils.unescapeJava(input.getString(4));
            System.out.println(string);
            //
            JSONObject object = (JSONObject) RaterBolt.PARSER.parse(string.substring(1, string.length() -1));

            //
            String time = (String) object.get("timestamp");
            Double rate = (Double) object.get("rate");

            this.collector.emit(new Values(time, rate));
        } catch (Exception e) {
            System.err.println(e);
        } finally {
            this.collector.ack(input);
        }
    }
}
