package org.knoesis.semmed.concept.pairfilter.generator;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.knoesis.semmed.concept.pairfilter.generator.output.SetFileOutputFormat;

public class Driver extends Configured implements Tool {

    private static final String KEY_HADOOP_JOBNAME = "hadoop.jobname";
    private static final String KEY_HADOOP_INPUT_DIRS = "hadoop.inputdirs";
    private static final String KEY_HADOOP_PAIR_FILTER_DIR = "hadoop.pairfilterdir";

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new Driver(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        if (args.length < 1) {
            throw new ConfigurationException("Configuration XML not specified.");
        }

        XMLConfiguration xmlConf = new XMLConfiguration(args[0]);
        String jobName = xmlConf.getString(KEY_HADOOP_JOBNAME, "");
        String inputDirs = xmlConf.getString(KEY_HADOOP_INPUT_DIRS);
        Path filterDir = new Path(xmlConf.getString(KEY_HADOOP_PAIR_FILTER_DIR));

        Configuration conf = getConf();
        Job job = Job.getInstance(conf);
        if (!jobName.isEmpty()) {
            job.setJobName(jobName);
        }
        job.setJarByClass(Driver.class);
        job.setMapperClass(PairFilterMapper.class);
        job.setCombinerClass(PairFilterReducer.class);
        job.setReducerClass(PairFilterReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        // important, since we want a single SetFile (directory) instead of one per reducer
        job.setNumReduceTasks(1);

        TextInputFormat.setInputPaths(job, inputDirs);

        job.setOutputFormatClass(SetFileOutputFormat.class);
        FileOutputFormat.setOutputPath(job, filterDir);

        return job.waitForCompletion(true) ? 0 : 1;
    }

}
