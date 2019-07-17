package it.unifi.batch;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ReducerKey implements WritableComparable<ReducerKey> {

    private Text query;
    private Center center;

    public ReducerKey() {
        query = new Text();
        center = new Center(144);
    }

    public ReducerKey(Text query, Center center) {
        this.query = query;
        this.center = new Center(center);
    }

    public Text getQuery() {
        return query;
    }

    public Center getCenter() {
        return center;
    }

    @Override
    public int compareTo(ReducerKey rk) {
        if(query.toString().equals(rk.getQuery().toString())) {
            if(center.getIndex().get() == rk.getCenter().getIndex().get()) {
                return 0;
            }
        }

        return 1;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(query.toString().length());
        dataOutput.writeChars(query.toString());

        try {
            dataOutput.writeInt(center.getListOfCoordinates().size());
        } catch(NullPointerException npe) {
            System.out.println("Test");
        }
        for (DoubleWritable p : center.getListOfCoordinates()) {
            dataOutput.writeDouble(p.get());
        }
        dataOutput.writeInt(center.getIndex().get());
        dataOutput.writeInt(center.getNumberOfPoints().get());
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        int queryLength = dataInput.readInt();

        StringBuilder query = new StringBuilder();

        for(int index = 0; index < queryLength; index++) {
            query.append(dataInput.readChar());
        }

        this.query = new Text(query.toString());

        int iParams = dataInput.readInt();
        List<DoubleWritable> listOfCoordinates = new ArrayList<>();
        for (int i = 0; i < iParams; i++) {
            listOfCoordinates.add(new DoubleWritable(dataInput.readDouble()));
        }
        center.setListOfCoordinates(listOfCoordinates);

        center.setIndex(new IntWritable(dataInput.readInt()));
        center.setNumberOfPoints(new IntWritable(dataInput.readInt()));
    }
}
