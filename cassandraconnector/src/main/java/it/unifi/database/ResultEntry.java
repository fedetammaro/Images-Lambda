package it.unifi.database;

public class ResultEntry {
    private String query;
    private String path;
    private Boolean isBatch;
    private int numImages;

    public ResultEntry() {}

    public ResultEntry(String query, String path, Boolean isBatch, int numImages) {
        this.query = query;
        this.path = path;
        this.isBatch = isBatch;
        this.numImages = numImages;
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

    public Boolean getBatch() {
        return isBatch;
    }

    public void setBatch(Boolean batch) {
        isBatch = batch;
    }

    public int getNumImages() {
        return numImages;
    }

    public void setNumImages(int numImages) {
        this.numImages = numImages;
    }
}
