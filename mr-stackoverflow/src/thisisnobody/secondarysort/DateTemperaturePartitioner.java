package thisisnobody.secondarysort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * The DateTemperaturePartitioner is a custom partitioner class, whcih
 * partitions data by the natural key only (using the yearMonth). Without custom
 * partitioner, Hadoop will partition your mapped data based on a hash code.
 *
 * In Hadoop, the partitioning phase takes place after the map() phase and
 * before the reduce() phase
 *
 * comment by zlp
 * 将DateTemperaturePair作为map输出的key，温度值作为map输出的value
 * mapreduce框架会自动按照key进行分区，洗牌，排序
 * 
 * 这个类实现的是分区部分
 * 因为reduce结果key为YearMonth，所以按照YearMonth的hashCode()做hash函数分区
 * 
 *
 *
 * @author Mahmoud Parsian
 *
 */
public class DateTemperaturePartitioner extends Partitioner<DateTemperaturePair, Text> {

	@Override
	public int getPartition(DateTemperaturePair pair, Text text, int numberOfPartitions) {
		// make sure that partitions are non-negative
		return Math.abs(pair.getYearMonth().hashCode() % numberOfPartitions);
	}
}
