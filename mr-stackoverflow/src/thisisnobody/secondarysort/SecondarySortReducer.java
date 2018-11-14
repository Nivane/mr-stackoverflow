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
	 * DateTemperatureGroupingComparatorʵ�����������DateTemperaturePair��YearMonth��ͬ���Ͱ�value�ϲ���һ��
	 * �������ǵ�����DateTemperaturePair(��ʹtemperature��һ��)��value���Ǳ��ϲ���
	 * ������ϲ��Ǳ������ĸ�DateTemperaturePair���ֶ������ĸ�DateTemperaturePair
	 * ������DateTemperaturePair�����������ǰ����Ǹ�
	 */
	@Override
	protected void reduce(DateTemperaturePair key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		System.out.println("һ���м���key? ������ĸ�key?");
		System.out.println(key.getTemperature());
		StringBuilder builder = new StringBuilder();
		for (Text value : values) {
			builder.append(value.toString());
			builder.append(",");
		}
		context.write(key.getYearMonth(), new Text(builder.toString()));
	}
}
