import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/*
 * DFReducer
 *
 * This reducer receives a word as the key and a list of document names
 * in which that word appears.
 *
 * It counts the number of unique documents for each word.
 *
 * Example input:
 *     king   [Hamlet.txt, Hamlet.txt, Midsummer.txt, AllsWell.txt]
 *
 * Example output:
 *     king   3
 *
 * This is the document frequency (DF) used later in TF-IDF.
 */
public class DFReducer extends Reducer<Text, Text, Text, IntWritable> {

    /*
     * reduce
     *
     * Uses a HashSet to ensure each document is counted only once
     * for a given word.
     *
     * key      - word
     * values   - list of document names containing the word
     * context  - Hadoop context used to write reducer output
     */
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        // Store unique document names only.
        HashSet<String> uniqueDocs = new HashSet<String>();

        for (Text val : values) {
            uniqueDocs.add(val.toString());
        }

        // Emit the number of unique documents containing this word.
        context.write(key, new IntWritable(uniqueDocs.size()));
    }
}