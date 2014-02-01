package org.knoesis.semmed.concept;

import java.net.URI;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Driver extends Configured implements Tool {

    private static final String DEFAULT_DB_DRIVER = "com.db.jdbc.Driver";
    private static final String DEFAULT_DB_TABLENAME = "CONCEPT_AGGREGATION";
    private static final String DEFAULT_DB_SCHEME = "jdbc:mysql";
    private static final String DEFAULT_DB_HOST = "localhost";
    private static final int DEFAULT_DB_PORT = 3306;

    private static final String KEY_DB_DRIVER = "db.driverclass";
    private static final String KEY_DB_SCHEME = "db.scheme";
    private static final String KEY_DB_HOST = "db.host";
    private static final String KEY_DB_PORT = "db.port";
    private static final String KEY_DB_DATABASE = "db.database";
    private static final String KEY_DB_USER = "db.user";
    private static final String KEY_DB_PASSWORD = "db.password";
    private static final String KEY_DB_TABLENAME = "db.tablename";
    private static final String KEY_HADOOP_JOBNAME = "hadoop.jobname";

    private static final String[] DB_FIELDS = {
        "CC1",
        "CC2",
        "PMID",
        "SID"
    };

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Driver(), args);
    }

    public int run(String[] args) throws Exception {
        if (args.length < 1) {
            throw new ConfigurationException("Configuration XML not specified.");
        }

        XMLConfiguration userConf = new XMLConfiguration(args[0]);
        String dbDriver = userConf.getString(KEY_DB_DRIVER, DEFAULT_DB_DRIVER);
        String dbScheme = userConf.getString(KEY_DB_SCHEME, DEFAULT_DB_SCHEME);
        String dbHost = userConf.getString(KEY_DB_HOST, DEFAULT_DB_HOST);
        int dbPort = userConf.getInt(KEY_DB_PORT, DEFAULT_DB_PORT);
        String dbDatabase = userConf.getString(KEY_DB_DATABASE);
        String dbUser = userConf.getString(KEY_DB_USER);
        String dbPassword = userConf.getString(KEY_DB_PASSWORD);
        String dbTableName = userConf.getString(KEY_DB_TABLENAME, DEFAULT_DB_TABLENAME);

        Configuration conf = getConf();
        String dbUrl = new URI(String.format("%s://%s:%d/%s", dbScheme, dbHost, dbPort, dbDatabase)).toString();
        DBConfiguration.configureDB(conf, dbDriver, dbUrl, dbUser, dbPassword);

        Job job = Job.getInstance(conf, userConf.getString(KEY_HADOOP_JOBNAME));
        DBOutputFormat.setOutput(job, dbTableName, DB_FIELDS);
        job.setOutputFormatClass(DBOutputFormat.class);
        job.setInputFormatClass(NLineInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(ConceptCoocurrence.class);
        job.setOutputKeyClass(ConceptCoocurrence.class);
        job.setOutputValueClass(NullWritable.class);
        job.setJarByClass(Driver.class);
        job.setMapperClass(ConceptMapper.class);
        job.setReducerClass(ConceptReducer.class);
        return job.waitForCompletion(true) ? 0 : 1;

    }

}
