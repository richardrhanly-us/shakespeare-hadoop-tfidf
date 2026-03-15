import java.io.IOException;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/*
 * TFIDFMapper
 *
 * This mapper is used in Job 3.
 *
 * It can process two kinds of input:
 *
 * 1. TF data from Job 1:
 *      document@word    tf
 *
 * 2. DF data from Job 2:
 *      word    df
 *
 * The mapper tags each record so the reducer can tell the difference:
 *
 * TF record emitted as:
 *      word    TF@document=tf
 *
 * DF record emitted as:
 *      word    DF@df
 *
 * This allows the reducer to join TF and DF values by word.
 */
public class TFIDFMapper extends Mapper<Object, Text, Text, Text> {

    private Text outKey = new Text();
    private Text outValue = new Text();

    /*
     * map
     *
     * Determines whether the current line came from TF output or DF output
     * based on the file path, then emits tagged records for joining in the reducer.
     *
     * key      - byte offset of the line (not used here)
     * value    - one input line
     * context  - Hadoop context used to write mapper output
     */
    @Override
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {


        String[] parts = value.toString().split("\\s+");

        // Skip malformed lines.
        if (parts.length < 2) {
            return;
        }

        // If the line came from the TF output, it will have document@word as the key.
        if (parts[0].contains("@")) {
            String[] docWord = parts[0].split("@");

            if (docWord.length != 2) {
                return;
            }

            String document = docWord[0];
            String word = docWord[1];
            String tf = parts[1];

            outKey.set(word);
            outValue.set("TF@" + document + "=" + tf);
            context.write(outKey, outValue);
        } else {
            // Otherwise treat it as DF output: word df
            String word = parts[0];
            String df = parts[1];

            outKey.set(word);
            outValue.set("DF@" + df);
            context.write(outKey, outValue);
        }
    }
}