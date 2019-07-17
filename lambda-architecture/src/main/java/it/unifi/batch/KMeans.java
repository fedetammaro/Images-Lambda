package it.unifi.batch;

import it.unifi.database.CassandraDriver;
import it.unifi.database.FeatureEntry;
import net.semanticmetadata.lire.imageanalysis.features.global.CEDD;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.*;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.*;
import java.net.URI;
import java.util.*;

public class KMeans {

    private static int ceddNumberOfFeatures = 144;
    private static String hdfsUri = "hdfs://localhost:9000/";

    public static void main(String[] args) throws Exception {
        CassandraDriver.connect();
        CassandraDriver.createResultsTable();
        CassandraDriver.createFeaturesTable();

        String query = args[0];
        Path input = new Path("/input/" + query);
        Path dataset = new Path("/dataset/" + query);
        Path antiLoop = new Path("/" + query + "-antiloop");
        Path output = new Path("/output");
        Path centers = new Path("/centers/" + query + "-centers.seq");

        ArrayList<ArrayList<DoubleWritable>> points = new ArrayList<>();

        Configuration conf = new Configuration();
        conf.addResource(new Path("/home/federico/hadoop/etc/hadoop/core-site.xml"));
        conf.addResource(new Path("/home/federico/hadoop/etc/hadoop/hdfs-site.xml"));
        conf.set("fs.hdfs.impl",
                org.apache.hadoop.hdfs.DistributedFileSystem.class.getName()
        );
        conf.set("fs.file.impl",
                org.apache.hadoop.fs.LocalFileSystem.class.getName()
        );

        FileSystem fs = FileSystem.get(new URI(hdfsUri), conf);

        conf.set("centersFilePath", centers.toString());
        conf.set("antiLoopPath", antiLoop.toString());
        conf.set("hdfsUri", hdfsUri);
        conf.set("query", query);
        conf.setDouble("threshold", Double.parseDouble(args[1]));
        conf.setBoolean("mapOnly", false);
        conf.setDouble("oldAvgValue", Double.MAX_VALUE);
        conf.setDouble("oldAvgValue", 0.0);

        int k = 3;
        conf.setInt("k", k);
        conf.setInt("iCoordinates", ceddNumberOfFeatures);

        if (fs.exists(antiLoop)) {
            System.out.println("Antiloop file deleted.");
            fs.delete(antiLoop, true);
        }

        if (fs.exists(dataset)) {
            System.out.println("Dataset deleted.");
            fs.delete(dataset, true);
        }

        if (fs.exists(centers)) {
            System.out.println("Centers deleted.");
            fs.delete(centers, true);
        }

        if (fs.exists(output)) {
            System.out.println("Old output folder deleted.");
            fs.delete(output, true);
        }

        createInput(conf, fs, query, input, dataset, points);
        createCenters(k, conf, centers, points);

        long isConverged = 0;
        int iterations = 0;
        while (isConverged != 1) {
            fs.delete(new Path(hdfsUri + output.toString()), true);
            Job job = Job.getInstance(conf, "K means iter");
            startJob(job, dataset, output);

            isConverged = job.getCounters().findCounter(Reduce.CONVERGE_COUNTER.CONVERGED).getValue();

            iterations++;
            conf.setBoolean("firstIteration", false);
        }

        conf.setBoolean("mapOnly", true);

        fs.delete(output, true);
        Job job = Job.getInstance(conf, "Last map");
        startJob(job, dataset, output);

        fs.delete(new Path(hdfsUri + dataset.toString()), true);
        fs.delete(new Path(hdfsUri + antiLoop.toString()), true);
        System.out.println("Number of iterations\t" + iterations);

        CassandraDriver.close();
        fs.close();
    }

    private static void createCenters(int k, Configuration conf, Path centers, ArrayList<ArrayList<DoubleWritable>> points) throws IOException {
        SequenceFile.Writer centerWriter = SequenceFile.createWriter(conf,
                SequenceFile.Writer.file(new Path(hdfsUri + centers.toString())),
                SequenceFile.Writer.keyClass(IntWritable.class),
                SequenceFile.Writer.valueClass(Center.class));
        Random r = new Random();

        int randomIndex = r.nextInt(points.size());
        ArrayList<DoubleWritable> randomPoint = points.remove(randomIndex);
        ArrayList<Center> chosenCenters = new ArrayList<>();
        chosenCenters.add(new Center(randomPoint, new IntWritable(0), new IntWritable(0)));
        centerWriter.append(0, chosenCenters.get(0));
        ArrayList<Double> distances = new ArrayList<>();

        for(int index = 1; index < k; index++) {
            distances.clear();
            double distance;
            double sum = 0.0;

            for(ArrayList<DoubleWritable> point : points) {
                double minimumDistance = Double.MAX_VALUE;
                for(Center cycleCenter : chosenCenters) {
                    distance = Distance.findDistanceDoubleWritable(cycleCenter.getListOfCoordinates(), point);
                    if(distance < minimumDistance) {
                        minimumDistance = distance;
                    }
                }

                distances.add(Math.pow(minimumDistance, 2) + sum);
                sum = Math.pow(minimumDistance, 2) + sum;
            }

            double randomValue = distances.get(distances.size() - 1) * r.nextDouble();
            int nextCenterIndex = 0;

            for(double distanceValue : distances) {
                if (distanceValue >= randomValue) {
                    nextCenterIndex = distances.indexOf(distanceValue);
                }
            }

            chosenCenters.add(new Center(points.remove(nextCenterIndex), new IntWritable(index), new IntWritable(0)));
            centerWriter.append(index, chosenCenters.get(index));
        }

        centerWriter.close();
    }

    private static void createInput(Configuration conf, FileSystem fs, String query, Path input, Path dataset, ArrayList<ArrayList<DoubleWritable>> points) throws IOException {
        FSDataInputStream inputStream = fs.open(new Path(input + "/paths"));
        FSDataOutputStream outputStream = fs.create(new Path(dataset + "/" + query + "-dataset"));

        String fileLines = IOUtils.toString(inputStream);
        String[] lines = fileLines.split("\n");

        ArrayList<FeatureEntry> featureEntries = new ArrayList<>();

        ArrayList<String> databaseFeatures = CassandraDriver.getFeatures(query);

        for(String databaseFeature : databaseFeatures) {
            outputStream.writeBytes(databaseFeature + "\n");
        }

        int numProcessedImages = 0;
        for(String line : lines) {
            String imageQuery = line.split(",")[0];
            String imagePath = line.split(",")[1];
            CEDD cedd = new CEDD();

            try {
                BufferedImage bufferedImage;

                bufferedImage = ImageIO.read(fs.open(new Path(imagePath)));
                cedd.extract(bufferedImage);
                double[] histogram = cedd.getFeatureVector();
                ArrayList<DoubleWritable> features = new ArrayList<>();
                for(double coordinate : histogram) {
                    features.add(new DoubleWritable(coordinate));
                }

                points.add(features);

                String histogramString = "";

                for(double feature : histogram) {
                    histogramString += feature + ";";
                }
                outputStream.writeBytes(imageQuery + "!" + imagePath + "!" + histogramString + "\n");
                featureEntries.add(new FeatureEntry(imageQuery, imagePath, histogramString));
                numProcessedImages++;
            } catch (Exception e) {
                System.out.println("Error processing image " + imagePath);
            }

        }

        CassandraDriver.insertFeatures(featureEntries);

        conf.setInt("numImages", databaseFeatures.size() + numProcessedImages);

        outputStream.close();
        fs.delete(new Path(input + "/paths"), false);
    }

    private static void startJob(Job job, Path dataset, Path output) throws IOException, InterruptedException, ClassNotFoundException {
        job.setJarByClass(KMeans.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(hdfsUri + dataset.toString()));
        FileOutputFormat.setOutputPath(job, new Path(hdfsUri + output.toString()));
        job.setMapOutputKeyClass(ReducerKey.class);
        job.setMapOutputValueClass(ImageFeature.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.waitForCompletion(true);
    }
}
