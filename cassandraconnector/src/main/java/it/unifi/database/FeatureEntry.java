package it.unifi.database;

public class FeatureEntry {
    private String query;
    private String path;
    private String features;

    public FeatureEntry(String query, String path, String features) {
        this.query = query;
        this.path = path;
        this.features = features;
    }

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getFeatures() {
        return features;
    }

    public void setFeatures(String features) {
        this.features = features;
    }
}
