/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.knoesis.semmed.concept.inputformat;

/**
 *
 * @author shreyansh
 */
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

class NLineReader extends RecordReader<NullWritable, Text> {

    private final LineRecordReader lineReader = new LineRecordReader();
    private final Text value = new Text();
    private boolean more = true;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException {
        lineReader.initialize(split, context);
    }

    @Override
    public boolean nextKeyValue() throws IOException {
        if (!more || !lineReader.nextKeyValue()) {
            more = false; // in case hadoop calls us an extra time
            return false;
        }
        String line = lineReader.getCurrentValue().toString();
        StringBuilder sb = new StringBuilder(line);
        while ((more = lineReader.nextKeyValue()) && !(line = lineReader.getCurrentValue().toString()).isEmpty()) {
            sb.append(line);
        }
        value.set(sb.toString());
        if (more) {
            lineReader.nextKeyValue(); // skip second newline
        }
        return true;
    }

    @Override
    public NullWritable getCurrentKey() throws IOException, InterruptedException {
        return NullWritable.get();
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException {
        return lineReader.getProgress();
    }

    @Override
    public void close() throws IOException {
        lineReader.close();
    }

}