package it.unifi.speed;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.net.URI;
import java.util.HashSet;
import java.util.Map;

public class ImageReaderSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;
    private static String query;
    private static final String hdfsUri = "hdfs://localhost:9000";
    private static final HashSet<String> seenImages = new HashSet<>();

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        query = (String)conf.get("query");
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("query", "path"));
    }

    public void nextTuple() {
        try {
            FileSystem fs = FileSystem.get(new URI(hdfsUri), new Configuration());
            RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(new Path(hdfsUri + "/images/" + query + "/"), false);

            while(iterator.hasNext()) {
                String cleanFileName = iterator.next().getPath().toString().replace(hdfsUri, "");

                if(!seenImages.contains(cleanFileName) && !cleanFileName.contains("COPYING")) {
                    seenImages.add(cleanFileName);
                    System.out.println("Spout emitted a new file: " + cleanFileName);
                    collector.emit(new Values(query, cleanFileName));
                }
            }
        } catch (Exception e) {
            System.out.println("Spout: no images to process");
        }
    }
}
