package it.unifi.database;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import java.util.ArrayList;

public class CassandraDriver {
    private static Cluster cluster;
    private static Session session;

    public static void connect() {
        if(session == null && cluster == null) {
            Cluster.Builder b = Cluster.builder().withoutJMXReporting().addContactPoint("127.0.0.1").withPort(9042);
            cluster = b.build();

            session = cluster.connect();
            System.out.println("===CONNECTING TO CASSANDRA==");
        }
    }

    public static void close() {
        if(session != null && cluster != null) {
            session.close();
            cluster.close();
            session = null;
            cluster = null;
            System.out.println("===CLOSING CASSANDRA CONNECTION==");
        }
    }

    public static void createKeyspace(String keyspaceName, String replicationStrategy, int replicationFactor) {
        StringBuilder sb = new StringBuilder("CREATE KEYSPACE IF NOT EXISTS ")
                .append(keyspaceName).append(" WITH replication = {")
                .append("'class':'").append(replicationStrategy)
                .append("','replication_factor':").append(replicationFactor)
                .append("};");

        String query = sb.toString();
        session.execute(query);
    }

    public static void createResultsTable() {
        StringBuilder sb = new StringBuilder("CREATE TABLE IF NOT EXISTS ")
                .append("lambda.results").append("(")
                .append("query text,")
                .append("path text,")
                .append("isBatch Boolean,")
                .append("time timestamp,")
                .append("numImages int,")
                .append("primary key(query, path));");

        String query = sb.toString();
        session.execute(query);
    }

    public static void createFeaturesTable() {
        StringBuilder sb = new StringBuilder("CREATE TABLE IF NOT EXISTS ")
                .append("lambda.features").append("(")
                .append("query text,")
                .append("path text,")
                .append("features text,")
                .append("primary key(query, path, features));");

        String query = sb.toString();
        session.execute(query);
    }

    public static void insertResults(ArrayList<ResultEntry> results) {
        String query = results.get(0).getQuery();

        StringBuilder sb = new StringBuilder("SELECT * FROM lambda.results WHERE query = '")
                .append(query)
                .append("';");

        ResultSet rs = session.execute(sb.toString());

        for(Row row : rs) {
            sb = new StringBuilder("DELETE FROM lambda.results WHERE query = '")
                    .append(query)
                    .append("' AND path = '")
                    .append(row.getString("path"))
                    .append("' IF EXISTS;");
            session.execute(sb.toString());
            System.out.println("Row deleted");
        }

        for(ResultEntry result : results) {
            sb = new StringBuilder("INSERT INTO ")
                    .append("lambda.results").append("(query, path, isBatch, time, numImages) ")
                    .append("VALUES ('")
                    .append(result.getQuery())
                    .append("', '")
                    .append(result.getPath())
                    .append("', ")
                    .append(result.getBatch())
                    .append(", ")
                    .append("toTimeStamp(now())")
                    .append(", ")
                    .append(result.getNumImages())
                    .append(");");

            session.execute(sb.toString());
        }
    }

    public static void insertFeatures(ArrayList<FeatureEntry> features) {
        StringBuilder sb;

        for(FeatureEntry feature : features) {
            sb = new StringBuilder("INSERT INTO ")
                    .append("lambda.features").append("(query, path, features) ")
                    .append("VALUES ('")
                    .append(feature.getQuery())
                    .append("', '")
                    .append(feature.getPath())
                    .append("', '")
                    .append(feature.getFeatures())
                    .append("');");

            session.execute(sb.toString());
        }
    }

    public static ArrayList<ResultEntry> getResults(String query) {
        ArrayList<ResultEntry> results = new ArrayList<>();

        StringBuilder sb = new StringBuilder("SELECT * FROM lambda.results WHERE query = '")
                .append(query)
                .append("';");

        ResultSet rs = session.execute(sb.toString());

        for(Row row : rs) {
            ResultEntry resultEntry = new ResultEntry();
            resultEntry.setQuery(row.getString("query"));
            resultEntry.setPath(row.getString("path"));
            resultEntry.setBatch(row.getBool("isBatch"));
            resultEntry.setNumImages(row.getInt("numImages"));

            results.add(resultEntry);
        }

        return results;
    }

    public static ArrayList<String> getFeatures(String query) {
        ArrayList<String> features = new ArrayList<>();

        StringBuilder sb = new StringBuilder("SELECT * FROM lambda.features WHERE query = '")
                .append(query)
                .append("';");

        ResultSet rs = session.execute(sb.toString());

        for(Row row : rs) {
            sb = new StringBuilder(row.getString("query"))
                    .append("!")
                    .append(row.getString("path"))
                    .append("!")
                    .append(row.getString("features"));
            features.add(sb.toString());
        }

        return features;
    }
}
