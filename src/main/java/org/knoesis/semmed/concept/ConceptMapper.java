/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.knoesis.semmed.concept;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author alan
 */
public class ConceptMapper extends Mapper<Text, Text, Text, ConceptCoocurrence> {

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {

    }

}
