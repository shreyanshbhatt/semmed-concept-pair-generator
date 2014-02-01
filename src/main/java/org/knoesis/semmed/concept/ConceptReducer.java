/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.knoesis.semmed.concept;

import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author alan
 */
public class ConceptReducer extends Reducer<Text, ConceptCoocurrence, ConceptCoocurrence, NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<ConceptCoocurrence> values, Context context) throws IOException, InterruptedException {
        for (ConceptCoocurrence cooccurrence : values) {
            context.write(cooccurrence, null);
        }
    }

}
