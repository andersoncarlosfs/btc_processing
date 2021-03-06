/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.btc.controller.bolts.printers;

import org.apache.storm.shade.org.json.simple.parser.JSONParser;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

/**
 *
 * @author ?
 */
public class JSONPrinterBolt extends BaseBasicBolt{

    private static final JSONParser PARSER = new JSONParser();
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        System.out.println(declarer);
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        try {
            System.out.println(PARSER.parse(input.getString(4)));
        } catch (Exception exception) {
            System.out.println(exception);
        }       
    }
    
}
