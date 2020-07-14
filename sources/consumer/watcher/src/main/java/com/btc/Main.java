/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.btc;

import com.btc.controller.bolts.ConverterBolt;
import com.btc.controller.bolts.IndexerBolt;
import com.btc.controller.bolts.SplitterBolt;
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
        // Rates
        // Kafka
        KafkaSpoutConfig.Builder<String, String> kafkaConfingRates = KafkaSpoutConfig.builder("localhost:9092", "rates");
        kafkaConfingRates.setProp(ConsumerConfig.GROUP_ID_CONFIG, "rates");
        builder.setSpout("rates_kafka_spout", new KafkaSpout<>(kafkaConfingRates.build()));

        // ElasticSearch         
        builder.setBolt("rates_elasticsearch_bolt", new IndexerBolt("rates")).shuffleGrouping("rates_kafka_spout");
        
        // Transactions
        // Kafka
        KafkaSpoutConfig.Builder<String, String> kafkaConfigTransactions = KafkaSpoutConfig.builder("localhost:9092", "transactions");
        kafkaConfigTransactions.setProp(ConsumerConfig.GROUP_ID_CONFIG, "transactions");
        builder.setSpout("transactions_kafka_spout", new KafkaSpout<>(kafkaConfigTransactions.build()));

        // Splitter
        builder.setBolt("objects_elasticsearch_bolt", new SplitterBolt()).shuffleGrouping("transactions_kafka_spout");
        
        // ElasticSearch         
        builder
                .setBolt("transactions_elasticsearch_bolt", new ConverterBolt("transactions"))
                .shuffleGrouping("objects_elasticsearch_bolt", "transactions")
                .shuffleGrouping("rates_kafka_spout");
        
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
