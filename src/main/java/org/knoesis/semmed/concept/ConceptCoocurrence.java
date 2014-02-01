/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.knoesis.semmed.concept;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

/**
 *
 * @author alan
 */
public class ConceptCoocurrence implements DBWritable, WritableComparable<ConceptCoocurrence> {

    private String pmid;
    private String sentenceId;
    private String firstConcept;
    private String secondConcept;

    public ConceptCoocurrence(String pmid, String sentenceId, String firstConcept, String secondConcept) {
        this.pmid = pmid;
        this.sentenceId = sentenceId;
        this.firstConcept = firstConcept;
        this.secondConcept = secondConcept;
    }

    public void write(PreparedStatement statement) throws SQLException {
        statement.setString(1, pmid);
        statement.setString(2, sentenceId);
        statement.setString(3, firstConcept);
        statement.setString(4, secondConcept);
    }

    public void readFields(ResultSet resultSet) throws SQLException {
        pmid = resultSet.getString(1);
        sentenceId = resultSet.getString(2);
        firstConcept = resultSet.getString(3);
        secondConcept = resultSet.getString(4);
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(pmid);
        out.writeUTF(sentenceId);
        out.writeUTF(firstConcept);
        out.writeUTF(secondConcept);
    }

    public void readFields(DataInput in) throws IOException {
        pmid = in.readUTF();
        sentenceId = in.readUTF();
        firstConcept = in.readUTF();
        secondConcept = in.readUTF();
    }

    public int compareTo(ConceptCoocurrence o) {
        int c = pmid.compareTo(o.pmid);
        if (c != 0) {
            return c;
        }
        c = sentenceId.compareTo(o.sentenceId);
        if (c != 0) {
            return c;
        }
        c = firstConcept.compareTo(o.firstConcept);
        if (c != 0) {
            return c;
        }
        return secondConcept.compareTo(o.secondConcept);
    }

}
