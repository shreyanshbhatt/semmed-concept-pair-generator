/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.knoesis.semmed.concept;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author alan
 */
public class ConceptMapper extends Mapper<NullWritable, Text, Text, ConceptCoocurrence> {

    @Override
    protected void map(NullWritable key, Text value, Context context) throws IOException, InterruptedException {

        String input = value.toString();
        StringTokenizer st = new StringTokenizer(input, "\n");
        String[] lines = new String[st.countTokens()];
        int i = 0;

        while (st.hasMoreTokens()) {
            lines[i++] = st.nextToken();
        }

        for (i = 0; i < lines.length; i++) {
            String[] splits1 = lines[i].split("\\|");
            if(!splits1[5].equals("entity")) {
                continue;
            }
            for (int j = i; j < lines.length; j++) {
                if (i == j) {
                    continue;
                }
          
                String[] splits2 = lines[j].split("\\|");
                
                if(!splits2[5].equals("entity")) {
                    continue;
                }
                
                String pmid = splits1[1];
                String geneid1 = splits1[9];
                String geneid2 = splits2[9];
                String cui1 = geneid1.isEmpty() ? splits1[6] : geneid1;
                String cui2 = geneid2.isEmpty() ? splits2[6] : geneid2;
                String sentenceid = splits1[4];

                context.write(null, new ConceptCoocurrence(pmid, sentenceid, cui1, cui2));
            }
        }
    }

}
