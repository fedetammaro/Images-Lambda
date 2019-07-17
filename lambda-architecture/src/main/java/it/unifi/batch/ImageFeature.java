package it.unifi.batch;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import javax.annotation.Nonnull;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ImageFeature implements WritableComparable<ImageFeature> {
    private Text image;
    private List<DoubleWritable> features;

    public ImageFeature() {
        image = new Text();
        features = new ArrayList<>();
    }

    public ImageFeature(ImageFeature imageFeature) {
        image = new Text(imageFeature.getImage());
        features = new ArrayList<>();

        for(DoubleWritable feature : imageFeature.getFeatures()) {
            features.add(new DoubleWritable(feature.get()));
        }
    }

    public ImageFeature(Path image, List<DoubleWritable> features) {
        this.image = new Text(image.toString());
        this.features = new ArrayList<>();

        this.features.addAll(features);
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(image.toString().length());
        dataOutput.writeChars(image.toString());
        dataOutput.writeInt(features.size());

        for(DoubleWritable feature : features) {
            dataOutput.writeDouble(feature.get());
        }
    }

    public void readFields(DataInput dataInput) throws IOException {
        int charLength = dataInput.readInt();

        StringBuilder imagePath = new StringBuilder();
        for(int index = 0; index < charLength; index++) {
            imagePath.append(dataInput.readChar());
        }

        image = new Text(imagePath.toString());

        int featuresLength = dataInput.readInt();

        features = new ArrayList<>();
        for(int index = 0; index < featuresLength; index++) {
            features.add(new DoubleWritable(dataInput.readDouble()));
        }
    }

    @Override
    public String toString() {
        StringBuilder returnString = new StringBuilder(image.toString() + ";");

        for(DoubleWritable feature : features) {
            returnString.append(feature.get()).append(";");
        }

        return returnString.toString();
    }


    public List<DoubleWritable> getFeatures() {
        return features;
    }

    public Text getImage() {
        return image;
    }

    @Override
    public int compareTo(@Nonnull ImageFeature imageFeature) {
        return 0;
    }
}
