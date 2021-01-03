package flow;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class FlowValue implements WritableComparable<FlowValue> {
	private int up;
	private int down;
	private int total;
	private int sum;
	public int getSum() {
		return sum;
	}

	public void setSum(int sum) {
		this.sum = sum;
	}

	public int getUp() {
		return up;
	}

	public void setUp(int up) {
		this.up = up;
	}

	public int getDown() {
		return down;
	}

	public void setDown(int down) {
		this.down = down;
	}

	public int getTotal() {
		return total;
	}

	public void setTotal(int total) {
		this.total = total;
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		this.up=arg0.readInt();
		this.down=arg0.readInt();
		this.total=arg0.readInt();
		this.sum=arg0.readInt();
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		arg0.writeInt(up);
		arg0.writeInt(down);
		arg0.writeInt(total);
		arg0.writeInt(sum);
	}

	@Override
	public int compareTo(FlowValue arg0) {
		if(this.total>arg0.total)
			return 1;
		if(this.total<arg0.total)
			return -1;
		return 0;
	}
	
}
