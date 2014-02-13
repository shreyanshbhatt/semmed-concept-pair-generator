/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package org.knoesis.semmed.concept.pairfilter.generator;

import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author alan
 */
public class PairFilterReducer extends Reducer<Text, NullWritable, Text, NullWritable> {

    private static final NullWritable NULL = NullWritable.get();

    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        // we're not interested in writing duplicates, so ignore all values
        context.write(key, NULL);
    }



}
