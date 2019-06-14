package com.atguigu.order;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class OrderGroupingComparator extends WritableComparator {

	protected OrderGroupingComparator() {
		super(OrderBean.class,true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		// 分组处理，只要传递过来的id相同，就默认为一组
		int result;
		OrderBean aBean = (OrderBean) a;
		OrderBean bBean = (OrderBean) b;
		if (aBean.getOrder_id() > bBean.getOrder_id()) {
			result = 1;
		} else if (aBean.getOrder_id() < bBean.getOrder_id()) {
			result = -1;
		} else {
			result = 0;
		}
		return result;
	}
}
