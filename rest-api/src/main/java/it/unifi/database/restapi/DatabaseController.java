package it.unifi.database.restapi;

import it.unifi.database.CassandraDriver;
import it.unifi.database.ResultEntry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

import java.awt.*;
import java.io.IOException;
import java.io.StringWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

@RestController
public class DatabaseController {

    private static String hdfsUri = "hdfs://localhost:9000/";
    private static Process process;

    @CrossOrigin
    @GetMapping(value = "/getResults/{query}", produces = "application/json; charset=utf-8")
    public List<ResultEntry> getResults(@PathVariable String query) {
        CassandraDriver.connect();
        return CassandraDriver.getResults(query);
    }

    @CrossOrigin
    @GetMapping(value = "/getMostRelevant/{query}")
    public void startProcessing(@PathVariable String query) throws IOException {
        process = Runtime.getRuntime().exec("python3 /home/federico/ImagesLambdaArchitecture/driver.py " + query + " 50");
    }

    @CrossOrigin
    @GetMapping(value = "/killProcess")
    public void killProcess() {
        process.destroyForcibly();
    }

    @CrossOrigin
    @GetMapping(value = "/getImage/images/{query}/{name}", produces = MediaType.IMAGE_JPEG_VALUE)
    public @ResponseBody byte[] getImage(@PathVariable(name = "query") String query,
                                         @PathVariable(name = "name") String name) throws URISyntaxException, IOException {
        Configuration conf = new Configuration();
        conf.addResource(new org.apache.hadoop.fs.Path("/home/federico/hadoop/etc/hadoop/core-site.xml"));
        conf.addResource(new Path("/home/federico/hadoop/etc/hadoop/hdfs-site.xml"));
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

        System.out.println("Serving image " + name);

        FileSystem fs = FileSystem.get(new URI(hdfsUri), conf);

        return IOUtils.readFullyToByteArray(fs.open(new Path("/images/" + query + "/" + name)));
    }
}
