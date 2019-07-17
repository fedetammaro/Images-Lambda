package it.unifi.batch;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;

import javax.annotation.Nonnull;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Point implements WritableComparable<Center> {

    private List<DoubleWritable> listOfCoordinates;

    Point(List<DoubleWritable> listOfCoordinates) {
        this.listOfCoordinates = new ArrayList<>();
        this.listOfCoordinates.addAll(listOfCoordinates);
    }

    Point() {
        listOfCoordinates = new ArrayList<>();
    }

    Point(int n) {
        listOfCoordinates = new ArrayList<>();
        for (int i = 0; i < n; i++)
            listOfCoordinates.add(new DoubleWritable(0.0));
    }

    public void readFields(DataInput dataInput) throws IOException {
        int iParams = dataInput.readInt();
        listOfCoordinates = new ArrayList<>();
        for (int i = 0; i < iParams; i++) {
            listOfCoordinates.add(new DoubleWritable(dataInput.readDouble()));
        }
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(listOfCoordinates.size());
        for (DoubleWritable p : listOfCoordinates) {
            dataOutput.writeDouble(p.get());
        }
    }

    public String toString() {
        String elements = "";
        for (DoubleWritable e : listOfCoordinates) {
            elements += e.get() + ";";
        }
        return elements;
    }

    public int compareTo(@Nonnull Center p) {
        return 0;
    }


    List<DoubleWritable> getListOfCoordinates() {
        return listOfCoordinates;
    }

    void setListOfCoordinates(List<DoubleWritable> listOfCoordinates) {
        this.listOfCoordinates.clear();
        this.listOfCoordinates.addAll(listOfCoordinates);
    }

}
