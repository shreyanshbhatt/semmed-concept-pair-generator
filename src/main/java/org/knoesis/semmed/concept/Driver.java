package org.knoesis.semmed.concept;

import java.net.URI;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.knoesis.semmed.concept.input.SemMedInputFormat;
import org.knoesis.semmed.concept.pairfilter.PairFilter;

public class Driver extends Configured implements Tool {

    private static final String DEFAULT_DB_DRIVER = "com.mysql.jdbc.Driver";
    private static final String DEFAULT_DB_SCHEME = "jdbc:mysql";
    private static final String DEFAULT_DB_HOST = "localhost";
    private static final boolean DEFAULT_DB_OUTPUT = false;
    private static final int DEFAULT_DB_PORT = 3306;

    private static final String KEY_DB_DRIVER = "db.driverclass";
    private static final String KEY_DB_SCHEME = "db.scheme";
    private static final String KEY_DB_HOST = "db.host";
    private static final String KEY_DB_PORT = "db.port";
    private static final String KEY_DB_DATABASE = "db.database";
    private static final String KEY_DB_USER = "db.user";
    private static final String KEY_DB_PASSWORD = "db.password";
    private static final String KEY_DB_TABLENAME = "db.tablename";
    private static final String KEY_HADOOP_DB_OUTPUT = "hadoop.dboutput";
    private static final String KEY_HADOOP_JOBNAME = "hadoop.jobname";
    private static final String KEY_HADOOP_NUM_REDUCERS = "hadoop.numreducers";
    private static final String KEY_HADOOP_INPUT_DIRS = "hadoop.inputdirs";
    private static final String KEY_HADOOP_OUTPUT_DIR = "hadoop.outputdir";
    private static final String KEY_HADOOP_PAIR_FILTER_DIR = "hadoop.pairfilterdir";

    private static final int NUM_DB_FIELDS = 5;

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new Driver(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        if (args.length < 1) {
            throw new ConfigurationException("Configuration XML not specified.");
        }

        Configuration conf = getConf();
        Job job = Job.getInstance(conf);

        XMLConfiguration xmlConf = new XMLConfiguration(args[0]);
        String jobName = xmlConf.getString(KEY_HADOOP_JOBNAME, "");
        String inputDirs = xmlConf.getString(KEY_HADOOP_INPUT_DIRS);
        String outputDir = xmlConf.getString(KEY_HADOOP_OUTPUT_DIR);
        String filterDir = xmlConf.getString(KEY_HADOOP_PAIR_FILTER_DIR);

        if (!jobName.isEmpty()) {
            job.setJobName(jobName);
        }
        if (xmlConf.containsKey(KEY_HADOOP_NUM_REDUCERS)) {
            job.setNumReduceTasks(xmlConf.getInt(KEY_HADOOP_NUM_REDUCERS,
                    job.getNumReduceTasks()));
        }
        FileInputFormat.setInputPaths(job, inputDirs);
        job.setJarByClass(Driver.class);
        job.setInputFormatClass(SemMedInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(ConceptCoocurrence.class);
        job.setOutputKeyClass(ConceptCoocurrence.class);
        job.setOutputValueClass(NullWritable.class);
        job.setMapperClass(ConceptMapper.class);
        job.setReducerClass(ConceptReducer.class);
        job.getConfiguration().set(PairFilter.FILTER_DIR, filterDir);

        boolean outputToDB = xmlConf.getBoolean(KEY_HADOOP_DB_OUTPUT, DEFAULT_DB_OUTPUT);
        if (outputToDB) {
            job.setOutputFormatClass(DBOutputFormat.class);
            String dbDriver = xmlConf.getString(KEY_DB_DRIVER, DEFAULT_DB_DRIVER);
            String dbScheme = xmlConf.getString(KEY_DB_SCHEME, DEFAULT_DB_SCHEME);
            String dbHost = xmlConf.getString(KEY_DB_HOST, DEFAULT_DB_HOST);
            int dbPort = xmlConf.getInt(KEY_DB_PORT, DEFAULT_DB_PORT);
            String dbDatabase = xmlConf.getString(KEY_DB_DATABASE);
            String dbUser = xmlConf.getString(KEY_DB_USER);
            String dbPassword = xmlConf.getString(KEY_DB_PASSWORD);
            String dbTableName = xmlConf.getString(KEY_DB_TABLENAME);
            job.setOutputFormatClass(DBOutputFormat.class);
            DBOutputFormat.setOutput(job, dbTableName, NUM_DB_FIELDS);
            String dbUrl = new URI(String.format("%s://%s:%d/%s", dbScheme, dbHost, dbPort, dbDatabase)).toString();
            DBConfiguration.configureDB(conf, dbDriver, dbUrl, dbUser, dbPassword);
        } else {
            job.setOutputFormatClass(TextOutputFormat.class);
            TextOutputFormat.setOutputPath(job, new Path(outputDir));
        }
        
        return job.waitForCompletion(true) ? 0 : 1;
    }

}
