import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/*
 * TFReducer
 *
 * This reducer receives all values for a given (document@word) key
 * and sums them to compute term frequency within that document.
 *
 * Example input:
 *     Hamlet.txt@king   [1, 1, 1, 1, 1]
 *
 * Example output:
 *     Hamlet.txt@king   5
 */
public class TFReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    /*
     * reduce
     *
     * Sums all the 1s associated with each document@word key.
     *
     * key      - document@word
     * values   - iterable list of 1s from the mapper
     * context  - Hadoop context used to write reducer output
     */
    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

        int sum = 0;

        // Add up all occurrences for this word in this document.
        for (IntWritable val : values) {
            sum += val.get();
        }

        // Emit final term frequency.
        context.write(key, new IntWritable(sum));
    }
}