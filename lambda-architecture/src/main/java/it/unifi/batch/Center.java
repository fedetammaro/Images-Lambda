package it.unifi.batch;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;

import javax.annotation.Nonnull;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

public class Center extends Point {

    private IntWritable index;

    private IntWritable numberOfPoints;

    Center(List<DoubleWritable> list, IntWritable index, IntWritable numberOfPoints) {
        super(list);
        this.index = new IntWritable(index.get());
        this.numberOfPoints = new IntWritable(numberOfPoints.get());
    }

    Center() {
        super();
    }

    Center(int n) {
        super(n);
        setNumberOfPoints(new IntWritable(0));
    }

    Center(List<DoubleWritable> l) {
        super(l);
        index = new IntWritable(0);
        numberOfPoints = new IntWritable(0);
    }

    Center(Center c) {
        super(c.getListOfCoordinates());
        setNumberOfPoints(c.getNumberOfPoints());
        setIndex(c.getIndex());
    }

    public void readFields(DataInput dataInput) throws IOException {
        super.readFields(dataInput);
        index = new IntWritable(dataInput.readInt());
        numberOfPoints = new IntWritable(dataInput.readInt());
    }

    public void write(DataOutput dataOutput) throws IOException {
        super.write(dataOutput);
        dataOutput.writeInt(index.get());
        dataOutput.writeInt(numberOfPoints.get());
    }

    @Override
    public int compareTo(@Nonnull Center c) {
        if (this.getIndex().get() == c.getIndex().get()) {
            return 0;
        }
        return 1;
    }

    boolean isConverged(Center c, Double threshold) {
        return threshold > Distance.findDistance(this, c);
    }

    public String toString() {
        return this.getIndex() + ";" + super.toString();
    }

    void divideCoordinates() {
        for (int i = 0; i < this.getListOfCoordinates().size(); i++) {
            this.getListOfCoordinates().set(i, new DoubleWritable(this.getListOfCoordinates().get(i).get() / numberOfPoints.get()));
        }
    }

    void addNumberOfPoints(IntWritable i) {
        this.numberOfPoints = new IntWritable(this.numberOfPoints.get() + i.get());
    }

    IntWritable getIndex() {
        return index;
    }

    IntWritable getNumberOfPoints() {
        return numberOfPoints;
    }

    void setIndex(IntWritable index) {
        this.index = new IntWritable(index.get());
    }

    void setNumberOfPoints(IntWritable numberOfPoints) {
        this.numberOfPoints = new IntWritable(numberOfPoints.get());
    }
}
