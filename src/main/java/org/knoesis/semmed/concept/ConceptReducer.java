package org.knoesis.semmed.concept;

import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ConceptReducer extends Reducer<Text, ConceptCoocurrence, ConceptCoocurrence, NullWritable> {

    private static final NullWritable NULL = NullWritable.get();
    
    @Override
    public void reduce(Text key, Iterable<ConceptCoocurrence> values, Context context) throws IOException, InterruptedException {
        for (ConceptCoocurrence cooccurrence : values) {
            context.write(cooccurrence, NULL);
        }
    }

}
