package thisisnobody.secondarysort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * SecondarySortReducer implements the reduce() function for the secondary sort
 * design pattern.
 *
 * @author Mahmoud Parsian
 *
 */
public class SecondarySortReducer extends Reducer<DateTemperaturePair, Text, Text, Text> {

	/**
	 * DateTemperatureGroupingComparator实现了如果两个DateTemperaturePair的YearMonth相同，就把value合并到一起
	 * 本来都是单独的DateTemperaturePair(即使temperature不一样)，value还是被合并了
	 * 那这个合并是保留了哪个DateTemperaturePair，又丢弃了哪个DateTemperaturePair
	 * 保留的DateTemperaturePair是排序后在最前面的那个
	 */
	@Override
	protected void reduce(DateTemperaturePair key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		System.out.println("一共有几个key? 这个是哪个key?");
		System.out.println(key.getTemperature());
		StringBuilder builder = new StringBuilder();
		for (Text value : values) {
			builder.append(value.toString());
			builder.append(",");
		}
		context.write(key.getYearMonth(), new Text(builder.toString()));
	}
}
