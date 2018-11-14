package thisisnobody.toolrunner;

import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 
 * 实现Tool接口使用ToolRunner将Configuration中的所有属性键值对打印出来
 */
public class ConfigurationPrinter extends Configured implements Tool{
	
	
	static {
		Configuration.addDefaultResource("hafs-site.xml");
		Configuration.addDefaultResource("core-site.xml");
		Configuration.addDefaultResource("yarn-site.xml");
	}
	
	@Override
	public int run(String[] args) throws Exception {
		
		Configuration conf = getConf();
		for(Entry<String, String> entry : conf){
			System.out.printf("%s=%s\n", entry.getKey(), entry.getValue());
		}
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		
		int exitCode = ToolRunner.run(new ConfigurationPrinter(), null);
		System.exit(exitCode);
	}

}
