package it.unifi.speed;

import net.semanticmetadata.lire.imageanalysis.features.global.CEDD;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.net.URI;

public class FeatureExtractorBolt extends BaseBasicBolt {

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("query", "path", "features"));
    }

    public void execute(Tuple tuple, BasicOutputCollector collector) {
        String query = tuple.getStringByField("query");
        String path = tuple.getStringByField("path");

        CEDD cedd = new CEDD();

        try {
            String hdfsUri = "hdfs://localhost:9000/";
            FileSystem fs = FileSystem.get(new URI(hdfsUri), new Configuration());

            BufferedImage bufferedImage;

            bufferedImage = ImageIO.read(fs.open(new Path(path)));
            cedd.extract(bufferedImage);
            double[] histogram = cedd.getFeatureVector();

            System.out.println("Processed image: " + path);
            collector.emit(new Values(query, path, histogram));
        } catch(Exception e) {
            System.out.println("Error processing image " + path);
        }
    }
}
