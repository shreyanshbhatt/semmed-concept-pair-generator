package org.knoesis.semmed.concept;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.knoesis.semmed.concept.pairfilter.PairFilter;
import org.knoesis.semmed.concept.pairfilter.UMLSPairFilter;

public class ConceptMapper extends Mapper<NullWritable, Text, Text, ConceptCoocurrence> {

    private final Text pmid = new Text();
    private PairFilter filter;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        Configuration conf = context.getConfiguration();
        filter = UMLSPairFilter.get(conf);
    }

    @Override
    protected void map(NullWritable key, Text value, Context context) throws IOException, InterruptedException {
        String input = value.toString();
        StringTokenizer st = new StringTokenizer(input, "\n");
        String[] lines = new String[st.countTokens()];
        int i = 0;

        while (st.hasMoreTokens()) {
            lines[i++] = st.nextToken();
        }

        for (i = 0; i < lines.length - 1; i++) {
            String[] splits1 = lines[i].split("\\|");
            if (splits1.length < 6) {
                System.out.println("SHORT LINE: " + lines[i]);
                continue;
            }
            if (!splits1[5].equals("entity")) {
                continue;
            }
            for (int j = i + 1; j < lines.length; j++) {

                String[] splits2 = lines[j].split("\\|");
                if (splits2.length < 6) {
                    System.out.println("SHORT LINE: " + lines[j]);
                    continue;
                }
                if (!splits2[5].equals("entity")) {
                    continue;
                }

                pmid.set(splits1[1]);
                String geneid1 = splits1[9];
                String geneid2 = splits2[9];
                String cui1 = splits1[6];
                String cui2 = splits2[6];
                int sentenceid1 = Integer.parseInt(splits1[4]);
                int sentenceid2 = Integer.parseInt(splits2[4]);

                // semantic type field may have multiple values (comma-separated)
                String[] semTypes1 = splits1[8].split(",");
                String[] semTypes2 = splits2[8].split(",");
                for (String semType1 : semTypes1) {
                    for (String semType2 : semTypes2) {
                        if (filter.accept(semType1, semType2)) {
                            context.write(pmid, new ConceptCoocurrence(pmid.toString(),
                                    geneid1.isEmpty() ? cui1 : geneid1,
                                    sentenceid1,
                                    geneid2.isEmpty() ? cui2 : geneid2,
                                    sentenceid2));
                            break;
                        }
                    }
                }
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        filter.close();
        super.cleanup(context);
    }

}
