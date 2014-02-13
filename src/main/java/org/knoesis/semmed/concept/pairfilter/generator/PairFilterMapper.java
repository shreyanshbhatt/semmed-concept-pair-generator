/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package org.knoesis.semmed.concept.pairfilter.generator;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author alan
 */
public class PairFilterMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    private final Text outKey = new Text();
    private static final NullWritable NULL = NullWritable.get();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer st = new StringTokenizer(value.toString());
        String semType1 = st.nextToken().toLowerCase();
        String semType2 = st.nextToken().toLowerCase();
        if (semType1.compareTo(semType2) < 0) {
            outKey.set(semType1 + "|" + semType2);
        } else {
            outKey.set(semType2 + "|" + semType1);
        }
        context.write(outKey, NULL);
    }

}
