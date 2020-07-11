/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.btc;

import com.btc.controller.bolts.RaterBolt;
import com.btc.controller.bolts.printers.BasicPrinterBolt;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.topology.TopologyBuilder;

/**
 *
 * @author ?
 */
public class Main {

    /**
     * 
     * @param args the command line arguments
     */
    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, AuthorizationException, Exception {
        TopologyBuilder builder = new TopologyBuilder();

        // Defining the spouts Configuration
        KafkaSpoutConfig.Builder<String, String> rates = KafkaSpoutConfig.builder("localhost:9092", "rates");

        // Building the topology
        builder.setSpout("rate_producer_kafka_spout", new KafkaSpout<>(rates.build()));
        builder.setBolt("rater_bolt", new RaterBolt()).shuffleGrouping("rate_producer_kafka_spout");
        builder.setBolt("printer_bolt", new BasicPrinterBolt()).shuffleGrouping("rater_bolt");

        Config config = new Config();
        
        String name = "btc";
        
        if (args.length > 0 && args[0].equals("remote")) {
            StormSubmitter.submitTopology(name, config, builder.createTopology());
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(name, config, builder.createTopology());
        }
    }

}
