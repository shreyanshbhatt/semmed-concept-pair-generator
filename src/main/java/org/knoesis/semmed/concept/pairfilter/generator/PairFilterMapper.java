/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package org.knoesis.semmed.concept.pairfilter.generator;

import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author alan
 */
public class PairFilterMapper extends Mapper<Text, Text, Text, NullWritable> {

    private static final NullWritable NULL = NullWritable.get();

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        context.write(key, NULL);
    }

}
