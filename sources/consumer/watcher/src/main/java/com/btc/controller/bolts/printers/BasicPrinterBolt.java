/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.btc.controller.bolts.printers;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

/**
 *
 * @author ?
 */
public class BasicPrinterBolt extends BaseBasicBolt{

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        System.out.println(declarer);
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        System.out.println(input);
    }
    
}
