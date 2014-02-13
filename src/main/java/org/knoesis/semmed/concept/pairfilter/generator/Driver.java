package org.knoesis.semmed.concept.pairfilter.generator;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.knoesis.semmed.concept.pairfilter.generator.output.SetFileOutputFormat;

public class Driver extends Configured implements Tool {

    private static final int DEFAULT_NUM_REDUCERS = 1;

    private static final String KEY_HADOOP_JOBNAME = "hadoop.jobname";
    private static final String KEY_HADOOP_NUM_REDUCERS = "hadoop.numreducers";
    private static final String KEY_HADOOP_INPUT_DIRS = "hadoop.inputdirs";
    private static final String KEY_HADOOP_PAIR_FILTER_DIR = "hadoop.pairfilterdir";

    private static final String SUFFIX_STAGING = "_staging";
    private static final String SUFFIX_SORT_STAGING = "_sort_staging";
    private static final String SUFFIX_PARTITIONS = "_partitions.lst";
    private static final String SUFFIX_TRANSFORM = "_transform";
    private static final String SUFFIX_SORT = "_totalsort";

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
        int numReducers = xmlConf.getInt(KEY_HADOOP_NUM_REDUCERS, DEFAULT_NUM_REDUCERS);
        String inputDirs = xmlConf.getString(KEY_HADOOP_INPUT_DIRS);
        String filterDir = xmlConf.getString(KEY_HADOOP_PAIR_FILTER_DIR);
        Path transformStaging = new Path(filterDir + SUFFIX_STAGING);
        Path sortStaging = new Path(filterDir + SUFFIX_SORT_STAGING);

        Configuration conf = getConf();
        Job transformationJob = Job.getInstance(conf);
        if (!jobName.isEmpty()) {
            transformationJob.setJobName(jobName + SUFFIX_TRANSFORM);
        }
        transformationJob.setJarByClass(Driver.class);
        transformationJob.setMapperClass(PairFilterMapper.class);
        transformationJob.setNumReduceTasks(0);
        transformationJob.setMapOutputKeyClass(Text.class);
        transformationJob.setMapOutputValueClass(NullWritable.class);
        transformationJob.setInputFormatClass(TextInputFormat.class);
        transformationJob.setOutputFormatClass(SequenceFileOutputFormat.class);

        TextInputFormat.setInputPaths(transformationJob, inputDirs);
        SequenceFileOutputFormat.setOutputPath(transformationJob, transformStaging);

        int result = transformationJob.waitForCompletion(true) ? 0 : 1;
        if (result == 0) {
            Job sortJob = Job.getInstance(conf);
            if (!jobName.isEmpty()) {
                sortJob.setJobName(jobName + SUFFIX_SORT);
            }
            sortJob.setJarByClass(Driver.class);
            sortJob.setMapperClass(Mapper.class);
            sortJob.setReducerClass(PairFilterReducer.class);
            sortJob.setNumReduceTasks(numReducers);

            sortJob.setOutputKeyClass(Text.class);
            sortJob.setOutputValueClass(NullWritable.class);

            sortJob.setInputFormatClass(SequenceFileInputFormat.class);
            SequenceFileInputFormat.setInputPaths(sortJob, transformStaging);

            if (numReducers == 1) {
                sortJob.setOutputFormatClass(SetFileOutputFormat.class);
                FileOutputFormat.setOutputPath(sortJob, new Path(filterDir));
                result = sortJob.waitForCompletion(true) ? 0 : 1;
            } else {
                sortJob.setPartitionerClass(TotalOrderPartitioner.class);
                Path partitionFile = new Path(filterDir + SUFFIX_PARTITIONS);
                TotalOrderPartitioner.setPartitionFile(sortJob.getConfiguration(), partitionFile);
                sortJob.setOutputFormatClass(SequenceFileOutputFormat.class);
                SequenceFileOutputFormat.setOutputPath(sortJob, sortStaging);
                InputSampler.writePartitionFile(sortJob,
                        new InputSampler.RandomSampler<Text, NullWritable>(result, numReducers));
                result = sortJob.waitForCompletion(true) ? 0 : 1;

                // need to launch a third job which merges sorted outputs
                if (result == 0) {
                    Job mergeJob = Job.getInstance(conf);
                    if (!jobName.isEmpty()) {
                        mergeJob.setJobName(jobName + SUFFIX_SORT);
                    }
                    mergeJob.setJarByClass(Driver.class);

                    mergeJob.setInputFormatClass(SequenceFileInputFormat.class);
                    SequenceFileInputFormat.setInputPaths(mergeJob, sortStaging);

                    mergeJob.setOutputFormatClass(SetFileOutputFormat.class);
                    FileOutputFormat.setOutputPath(mergeJob, new Path(filterDir));
                }
            }
        }

        // Delete temp files

        return result;
    }

}
