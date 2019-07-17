package it.unifi.speed;

import it.unifi.database.CassandraDriver;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class SpeedLayerTopology {

    public static void main(String[] args) throws Exception {
        System.out.println("===STARTING SPEED LAYER===");

        CassandraDriver.connect();
        CassandraDriver.createResultsTable();
        CassandraDriver.close();

        String query = args[0];

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("image_reader", new ImageReaderSpout(), 1);
        builder.setBolt("feature_extractor", new FeatureExtractorBolt(), 1)
                .fieldsGrouping("image_reader", new Fields("query"));
        builder.setBolt("centroid_evaluator", new EvaluateCentroidBolt(), 1)
                .fieldsGrouping("feature_extractor", new Fields("query"));

        LocalCluster cluster = new LocalCluster();
        Config conf = new Config();
        conf.put("query", query);
        cluster.submitTopology("speed-layer", conf, builder.createTopology());
    }
}
