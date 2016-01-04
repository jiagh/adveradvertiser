package vo;

import java.util.TreeSet;

public class RedisHistoryFrequency {

	TreeSet<Integer> historyTimeStamp = new TreeSet<Integer>();

	int value;

	public TreeSet<Integer> getHistoryTimeStamp() {
		return historyTimeStamp;
	}

	public void setHistoryTimeStamp(TreeSet<Integer> historyTimeStamp) {
		this.historyTimeStamp = historyTimeStamp;
	}

	public int getValue() {
		return value;
	}

	public void setValue(int value) {
		this.value = value;
	}

}
