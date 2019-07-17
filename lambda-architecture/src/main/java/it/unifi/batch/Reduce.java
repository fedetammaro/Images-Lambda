package it.unifi.batch;

import it.unifi.database.CassandraDriver;
import it.unifi.database.ResultEntry;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

public class Reduce extends Reducer<ReducerKey, ImageFeature, Text, Text> {
    private Logger logger = Logger.getLogger(Reduce.class);
    private HashMap<IntWritable, Center> newCenters = new HashMap<>();
    private HashMap<IntWritable, Center> oldCenters = new HashMap<>();
    private HashMap<Integer, HashSet<ImageFeature>> imageFeatureMap = new HashMap<>();
    private int iConvergedCenters = 0;

    public enum CONVERGE_COUNTER {
        CONVERGED
    }

    @Override
    public void reduce(ReducerKey key, Iterable<ImageFeature> values, Context context) {
        Configuration conf = context.getConfiguration();

        int numberOfPoints = 0;

        Iterator<ImageFeature> iterator = values.iterator();

        while(iterator.hasNext()) {
            int centerIndex = key.getCenter().getIndex().get();

            if(!imageFeatureMap.containsKey(centerIndex)) {
                HashSet<ImageFeature> imageFeatureHashSet = new HashSet<>();
                imageFeatureMap.put(centerIndex, imageFeatureHashSet);
            }

            imageFeatureMap.get(centerIndex).add(new ImageFeature(iterator.next()));

            numberOfPoints++;
        }

        key.getCenter().setNumberOfPoints(new IntWritable(numberOfPoints));

        Center newCenter = new Center(conf.getInt("iCoordinates", 2));

        boolean flagOld = false;

        if (newCenters.containsKey(key.getCenter().getIndex())) {
            newCenter = newCenters.get(key.getCenter().getIndex());
            flagOld = true;
        }

        int numElements = 0;
        double temp;

        for (ImageFeature imageFeature : imageFeatureMap.get(key.getCenter().getIndex().get())) {
            for (int i = 0; i < imageFeature.getFeatures().size(); i++) {
                temp = newCenter.getListOfCoordinates().get(i).get() + imageFeature.getFeatures().get(i).get();
                newCenter.getListOfCoordinates().get(i).set(temp);
            }

            numElements += key.getCenter().getNumberOfPoints().get();
        }
        newCenter.setIndex(key.getCenter().getIndex());
        newCenter.addNumberOfPoints(new IntWritable(numElements));

        if(!flagOld) {
            newCenters.put(newCenter.getIndex(), newCenter);
            oldCenters.put(key.getCenter().getIndex(), new Center(key.getCenter()));
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException {
        Configuration conf = context.getConfiguration();

        boolean mapOnly = conf.getBoolean("mapOnly", false);
        Double threshold = conf.getDouble("threshold", 0.5);
        int k = conf.getInt("k", 2);
        double avgValue = 0.0;
        double oldAvgValue = Double.MAX_VALUE;
        Path antiLoopPath = new Path(conf.get("antiLoopPath"));
        FileSystem fs = null;

        try {
            fs = FileSystem.get(new URI(conf.get("hdfsUri")), conf);
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }

        FSDataInputStream inputStream;
        ArrayList<ResultEntry> results = new ArrayList<>();

        if (!mapOnly) {
            if (fs.exists(antiLoopPath)) {
                inputStream = fs.open(antiLoopPath);
                oldAvgValue = Double.parseDouble(IOUtils.toString(inputStream));
                inputStream.close();
            } else {
                oldAvgValue = 0.0;
                FSDataOutputStream outputStream = fs.create(antiLoopPath);
                outputStream.close();
            }
            Path centersPath = new Path(conf.get("centersFilePath"));
            SequenceFile.Writer centerWriter = SequenceFile.createWriter(conf,
                    SequenceFile.Writer.file(new Path(conf.get("hdfsUri") + centersPath.toString())),
                    SequenceFile.Writer.keyClass(IntWritable.class),
                    SequenceFile.Writer.valueClass(Center.class));
            Iterator<Center> it = newCenters.values().iterator();
            Center newCenterValue;
            Center sameIndexC;
            while (it.hasNext()) {
                newCenterValue = it.next();
                newCenterValue.divideCoordinates();
                sameIndexC = oldCenters.get(newCenterValue.getIndex());
                if (newCenterValue.isConverged(sameIndexC, threshold))
                    iConvergedCenters++;
                avgValue += Math.pow(Distance.findDistanceDoubleWritable(newCenterValue.getListOfCoordinates(), sameIndexC.getListOfCoordinates()), 2);
                centerWriter.append(newCenterValue.getIndex(), newCenterValue);
            }
            avgValue = Math.sqrt(avgValue / newCenters.size());
            logger.fatal("Convergence value: " + avgValue);
            centerWriter.close();
        } else {
            newCenters = oldCenters;
        }

        if (iConvergedCenters == newCenters.keySet().size() || avgValue < threshold || avgValue == oldAvgValue) {
            context.getCounter(CONVERGE_COUNTER.CONVERGED).increment(1);

            if(mapOnly) {
                for (Center cycleCenter : newCenters.values()) {
                    String mostRelevantImage = "";
                    double minDistance = Double.MAX_VALUE;

                    for (ImageFeature imageFeature : imageFeatureMap.get(cycleCenter.getIndex().get())) {
                        Center compareCenter = new Center(cycleCenter);

                        double cycleDistance = Distance.findDistanceDoubleWritable(imageFeature.getFeatures(), compareCenter.getListOfCoordinates());

                        if (cycleDistance < minDistance) {
                            minDistance = cycleDistance;
                            mostRelevantImage = imageFeature.getImage().toString();
                        }
                    }

                    results.add(new ResultEntry(conf.get("query"), mostRelevantImage, true, conf.getInt("numImages", 0)));
                }

                CassandraDriver.insertResults(results);
            }
        }

        if(!mapOnly) {
            fs.delete(antiLoopPath, false);
            FSDataOutputStream outputStream = fs.create(antiLoopPath);
            outputStream.writeBytes(Double.toString(avgValue));
            outputStream.close();
        }

        logger.fatal("Converged centers: " + iConvergedCenters);
    }
}
