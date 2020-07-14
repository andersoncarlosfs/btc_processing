/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.btc;

import com.btc.controller.bolts.ConverterBolt;
import com.btc.controller.bolts.IndexerBolt;
import com.btc.controller.bolts.SplitterBolt;
import com.btc.controller.bolts.TransformerBolt;
import org.apache.kafka.clients.consumer.ConsumerConfig;
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
        // Building the topology
        TopologyBuilder builder = new TopologyBuilder();

        // Defining the spouts Configuration        
        // Kafka (Rates) [Spout]
        KafkaSpoutConfig.Builder<String, String> kafkaConfingRates = KafkaSpoutConfig.builder("localhost:9092", "rates");
        kafkaConfingRates.setProp(ConsumerConfig.GROUP_ID_CONFIG, "rates");
        builder.setSpout("rates", new KafkaSpout<>(kafkaConfingRates.build()));
        
        // Kafka (Transactions) [Spout]
        KafkaSpoutConfig.Builder<String, String> kafkaConfigTransactions = KafkaSpoutConfig.builder("localhost:9092", "transactions");
        kafkaConfigTransactions.setProp(ConsumerConfig.GROUP_ID_CONFIG, "transactions");
        builder.setSpout("transactions", new KafkaSpout<>(kafkaConfigTransactions.build()));

        // Transformer
        builder
                .setBolt("objects_storm_bolt", new TransformerBolt())
                .shuffleGrouping("rates")
                .shuffleGrouping("transactions");
        
        // Splitter
        builder
                .setBolt("transactions_storm_bolt", new SplitterBolt())
                .shuffleGrouping("objects_storm_bolt", "transactions");        
        
        // Converter         
        builder
                .setBolt("convertions_storm_bolt", new ConverterBolt())
                .shuffleGrouping("objects_storm_bolt", "rates")
                .shuffleGrouping("transactions_storm_bolt", "transactions");        
        
        // ElasticSearch         
        builder
                .setBolt("rates_elasticsearch_bolt", new IndexerBolt())
                .shuffleGrouping("objects_storm_bolt", "rates")
                .shuffleGrouping("transactions_storm_bolt", "blocks")
                .shuffleGrouping("convertions_storm_bolt", "transactions");
        
        // Configuring the topology
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
