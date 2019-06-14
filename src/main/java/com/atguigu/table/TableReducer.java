package com.atguigu.table;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TableReducer extends Reducer<Text, TableBean, TableBean, NullWritable>{

	
	@Override
	protected void reduce(Text key, Iterable<TableBean> values,
			Reducer<Text, TableBean, TableBean, NullWritable>.Context context) throws IOException, InterruptedException {
		ArrayList<TableBean> orderlist=new ArrayList<>();
		TableBean pdbean=new TableBean();
		for (TableBean tableBean : values) {
			if("order".equals(tableBean.getFlag())) {
				//订单表数据
				TableBean tem=new TableBean();
				try {
					BeanUtils.copyProperties(tem, tableBean);
					orderlist.add(tem);
				} catch (IllegalAccessException e) {
					e.printStackTrace();
				} catch (InvocationTargetException e) {
					e.printStackTrace();
				}
			}else {
				try {
					BeanUtils.copyProperties(pdbean, tableBean);
				} catch (IllegalAccessException e) {
					e.printStackTrace();
				} catch (InvocationTargetException e) {
					e.printStackTrace();
				}
			}
		}
		for (TableBean tableBean : orderlist) {
			tableBean.setPname(pdbean.getPname());
			context.write(tableBean, NullWritable.get());
		}
	}
}
