package org.knoesis.semmed.concept.inputformat;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class SemMedInputFormat extends FileInputFormat<NullWritable, Text> {

    @Override
    public boolean isSplitable(JobContext context, Path filename) {
        return false;
    }

    @Override
    public RecordReader<NullWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) {
        return new SemMedRecordReader();
    }

}
