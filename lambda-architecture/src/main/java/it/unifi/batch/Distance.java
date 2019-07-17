package it.unifi.batch;

import org.apache.hadoop.io.DoubleWritable;

import java.util.List;

class Distance {
    static Double findDistance(Point p1, Point p2) {
        int len = p1.getListOfCoordinates().size();

        List<DoubleWritable> l1 = p1.getListOfCoordinates();
        List<DoubleWritable> l2 = p2.getListOfCoordinates();

        double sum = 0.0;

        for (int i = 0; i < len; i++) {
            sum += Math.pow(l1.get(i).get() - l2.get(i).get(), 2);
        }

        return Math.sqrt(sum);
    }

    public static double findDistanceDoubleWritable(List<DoubleWritable> x, List<DoubleWritable> y) {
        double sum = 0;

        for(int index = 0; index < x.size(); index++) {
            sum += Math.pow(x.get(index).get() - y.get(index).get(), 2);
        }

        return Math.sqrt(sum);
    }

}
