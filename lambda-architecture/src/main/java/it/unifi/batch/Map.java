package it.unifi.batch;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Map extends Mapper<Object, Text, ReducerKey, ImageFeature> {
    private Logger logger = Logger.getLogger(Map.class);
    private List<Center> centers = new ArrayList<>();

    @Override
    protected void setup(Context context) throws IOException {
        Configuration conf = context.getConfiguration();
        Path centersPath = new Path(conf.get("centersFilePath"));

        SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(new Path(conf.get("hdfsUri") + centersPath.toString())));
        IntWritable key = new IntWritable();
        Center value = new Center();
        while (reader.next(key, value)) {
            Center c = new Center(value.getListOfCoordinates());
            c.setNumberOfPoints(new IntWritable(0));
            c.setIndex(key);
            centers.add(c);
        }
        reader.close();
        logger.fatal("Centers: " + centers.toString());
    }

    @Override
    public void map(Object key, Text value, Context context) {
        String line = value.toString();

        String query = line.split("!")[0];
        String imagePath = line.split("!")[1];
        String[] histogramString = line.split("!")[2].split(";");

        ArrayList<DoubleWritable> histogram = new ArrayList<>();

        for(String feature : histogramString) {
            histogram.add(new DoubleWritable(Double.parseDouble(feature)));
        }

        try {
            Point p = new Point(histogram);

            Center minDistanceCenter = null;
            Double minDistance = Double.MAX_VALUE;
            Double distanceTemp;
            for (Center c : centers) {
                distanceTemp = Distance.findDistance(c, p);
                if (minDistance > distanceTemp) {
                    minDistanceCenter = c;
                    minDistance = distanceTemp;
                }

            }

            ReducerKey reducerKey = new ReducerKey(new Text(query), new Center(minDistanceCenter));
            ImageFeature imageFeature = new ImageFeature(new Path(imagePath), histogram);

            context.write(reducerKey, imageFeature);
        } catch(Exception e) {
            System.out.println("Error processing image " + imagePath);
        }
    }


}
