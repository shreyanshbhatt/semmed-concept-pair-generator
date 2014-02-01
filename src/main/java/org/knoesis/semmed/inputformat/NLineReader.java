/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.knoesis.semmed.inputformat;

/**
 *
 * @author shreyansh
 */
import java.io.IOException;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.io.LongWritable;

class NLineReader implements RecordReader<Text, Text> {

    private LineRecordReader lineReader;
    private LongWritable linekey;
    private Text lineValue;

    public NLineReader(FileSplit fileSplit, Configuration conf) throws IOException {

        lineReader = new LineRecordReader(conf, fileSplit);
        lineValue = lineReader.createValue();
        linekey = lineReader.createKey();
    }

    @Override
    public boolean next(Text key, Text value) throws IOException {

        if (lineReader == null) {
            return false;
        }

        if (!lineReader.next(linekey, lineValue)) {
            return false;
        }

        String lines = null;

        while (!lineValue.toString().equals("")) {
            lines += lineValue.toString();
            if (!lineReader.next(linekey, lineValue)) {
                lineReader = null;
                break;
            }
        }

        key.set(linekey.toString());
        value.set(new Text(lines));

        return true;
    }

    @Override
    public Text createKey() {
        return new Text();
    }

    @Override
    public Text createValue() {
        return new Text();
    }

    @Override
    public long getPos() throws IOException {
        return lineReader.getPos();
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
