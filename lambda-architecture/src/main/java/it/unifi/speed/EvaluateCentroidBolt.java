package it.unifi.speed;

import it.unifi.database.CassandraDriver;
import it.unifi.database.ResultEntry;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

import java.util.*;

import static java.util.Map.Entry.comparingByValue;
import static java.util.stream.Collectors.toMap;

public class EvaluateCentroidBolt extends BaseBasicBolt {

    private static HashMap<String, HashMap<String, ArrayList<Double>>> pointsMap = new HashMap<>();
    private static HashMap<String, ArrayList<Double>> centroidsMap = new HashMap<>();
    private static boolean accessDatabase = true;
    private static int remainingVoidCycles = 0;

    public void prepare(Map conf, TopologyContext context) {
        System.out.println("===PREPARING BOLT===");
        try {
            CassandraDriver.connect();
        } catch(Exception e) {
            System.out.println("Error connecting to Cassandra database");
        }
    }

    public void cleanup() {
        System.out.println("==CLEANING UP BOLT===");
        CassandraDriver.close();
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    public void execute(Tuple tuple, BasicOutputCollector collector) {
        String query = tuple.getStringByField("query");
        String path = tuple.getStringByField("path");
        double[] histogram = (double[])tuple.getValueByField("features");

        ArrayList<Double> histogramList = new ArrayList<>();

        for(double feature : histogram) {
            histogramList.add(feature);
        }

        if(!pointsMap.containsKey(query)) {
            HashMap<String, ArrayList<Double>> point = new HashMap<>();
            point.put(path, histogramList);
            pointsMap.put(query, point);
        }

        pointsMap.get(query).put(path, histogramList);

        if(!centroidsMap.containsKey(query)) {
            centroidsMap.put(query, histogramList);
        } else {
            ArrayList<Double> centroidCoordinates = centroidsMap.get(query);

            ArrayList<Double> newCentroidCoordinates = new ArrayList<>();

            for(int index = 0; index < histogramList.size(); index++) {
                newCentroidCoordinates.add((centroidCoordinates.get(index) + histogramList.get(index))/2);
            }

            centroidsMap.replace(query, newCentroidCoordinates);
        }

        HashMap<String, ArrayList<Double>> allQueryImages = pointsMap.get(query);

        double distance = Double.MAX_VALUE;
        String mostRelevantImage = "";
        HashMap<String, Double> allPointsDistances = new HashMap<>();

        for(String imagePath : allQueryImages.keySet()) {
            double cycleDistance = L2(allQueryImages.get(imagePath), centroidsMap.get(query));
            allPointsDistances.put(imagePath, cycleDistance);
        }

        Map<String, Double> sortedPointsDistances = allPointsDistances
                .entrySet()
                .stream()
                .sorted(comparingByValue())
                .collect(toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e2, LinkedHashMap::new));

        ArrayList<ResultEntry> results = new ArrayList<>();
        int numResults = sortedPointsDistances.size() < 3 ? sortedPointsDistances.size() : 3;
        int cycleIndex = 0;

        for(String image : sortedPointsDistances.keySet()) {
            if(cycleIndex == numResults) {
                break;
            }

            results.add(new ResultEntry(query, image, false, sortedPointsDistances.size()));
            cycleIndex++;
        }

        if(accessDatabase) {
            ArrayList<ResultEntry> dbResults = CassandraDriver.getResults(query);
            if (dbResults.size() != 0 && dbResults.get(0).getBatch()) {
                remainingVoidCycles = dbResults.get(0).getNumImages();
                accessDatabase = false;
            } else {
                CassandraDriver.insertResults(results);
            }
        } else {
            remainingVoidCycles--;

            if(remainingVoidCycles == 0) {
                accessDatabase = true;

                CassandraDriver.insertResults(results);
            }
        }

        System.out.println(path + " - Most relevant image: " + mostRelevantImage + " with distance " + distance);
    }

    private static double L2(List<Double> x, List<Double> y) {
        double sum = 0;

        for(int index = 0; index < x.size(); index++) {
            sum += Math.pow(x.get(index) - y.get(index), 2);
        }

        return Math.sqrt(sum);
    }
}
