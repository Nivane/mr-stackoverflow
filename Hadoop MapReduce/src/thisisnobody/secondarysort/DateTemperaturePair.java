package thisisnobody.secondarysort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * The DateTemperaturePair class enable us to represent a composite type of
 * (yearMonth, day, temperature). To persist a composite type (actually any data
 * type) in Hadoop, it has to implement the org.apache.hadoop.io.Writable
 * interface.
 * 
 * To compare composite types in Hadoop, it has to implement the
 * org.apache.hadoop.io.WritableComparable interface.
 * 
 * comment by zlp 将DateTemperaturePair作为map输出的key，温度值作为map输出的value
 * mapreduce框架会自动按照key进行分区，洗牌，排序
 * 
 * 
 * 这个类实现的是排序部分 比较是为了将map输出按key排序，排序需要实现WritableComparable中的compare()方法
 * 对于本问题，排序的规则： 如果年月相同，则比较温度，温度小的在后面(降序)； 如果年月不同，则将年月排序，年月大的在后面(升序)
 * 
 *
 * @author Mahmoud Parsian
 *
 */
public class DateTemperaturePair implements WritableComparable<DateTemperaturePair> {

	private final Text yearMonth = new Text();
	private final Text day = new Text();
	private final IntWritable temperature = new IntWritable();

	public DateTemperaturePair() {
	}

	public DateTemperaturePair(String yearMonth, String day, int temperature) {
		this.yearMonth.set(yearMonth);
		this.day.set(day);
		this.temperature.set(temperature);
	}

	public static DateTemperaturePair read(DataInput in) throws IOException {
		DateTemperaturePair pair = new DateTemperaturePair();
		pair.readFields(in);
		return pair;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		yearMonth.write(out);
		day.write(out);
		temperature.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		yearMonth.readFields(in);
		day.readFields(in);
		temperature.readFields(in);
	}

	@Override
	public int compareTo(DateTemperaturePair pair) {
		int compareValue = this.yearMonth.compareTo(pair.getYearMonth());
		if (compareValue == 0) {
			compareValue = temperature.compareTo(pair.getTemperature());
			return -1 * compareValue;
		}
		return compareValue; // to sort ascending
		// return -1 * compareValue; // to sort descending
	}

	public Text getYearMonthDay() {
		return new Text(yearMonth.toString() + day.toString());
	}

	public Text getYearMonth() {
		return yearMonth;
	}

	public Text getDay() {
		return day;
	}

	public IntWritable getTemperature() {
		return temperature;
	}

	public void setYearMonth(String yearMonthAsString) {
		yearMonth.set(yearMonthAsString);
	}

	public void setDay(String dayAsString) {
		day.set(dayAsString);
	}

	public void setTemperature(int temp) {
		temperature.set(temp);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		DateTemperaturePair that = (DateTemperaturePair) o;
		if (temperature != null ? !temperature.equals(that.temperature) : that.temperature != null) {
			return false;
		}
		if (yearMonth != null ? !yearMonth.equals(that.yearMonth) : that.yearMonth != null) {
			return false;
		}

		return true;
	}

	@Override
	public int hashCode() {
		int result = yearMonth != null ? yearMonth.hashCode() : 0;
		result = 31 * result + (temperature != null ? temperature.hashCode() : 0);
		return result;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("DateTemperaturePair{yearMonth=");
		builder.append(yearMonth);
		builder.append(", day=");
		builder.append(day);
		builder.append(", temperature=");
		builder.append(temperature);
		builder.append("}");
		return builder.toString();
	}
}
