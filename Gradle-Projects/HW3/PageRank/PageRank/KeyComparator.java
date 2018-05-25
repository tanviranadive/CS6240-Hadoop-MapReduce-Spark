package PageRank;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

// used for sorting DoubleWritable page rank value
// in descending order
public class KeyComparator extends WritableComparator {

	protected KeyComparator() {
		super(DoubleWritable.class, true);
	}

	@Override
	public int compare(WritableComparable key1, WritableComparable key2) {
		DoubleWritable num1 = (DoubleWritable) key1;
		DoubleWritable num2 = (DoubleWritable) key2;

		if(num2.get() == num1.get())
			return 0;
		else {
			return num2.get() - num1.get() < 0? -1: 1;
		}
	}
}
