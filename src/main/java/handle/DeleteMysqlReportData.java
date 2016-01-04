package handle;

import java.sql.SQLException;
import offline.FixOfflineAnalysisRun;
import util.MysqlUtil;

public class DeleteMysqlReportData extends Thread {

	private MysqlUtil mt;

	public DeleteMysqlReportData() {

	}

	public MysqlUtil getMt() {
		return mt;
	}

	public void setMt(MysqlUtil mt) {
		this.mt = mt;
	}

	private String delSql = "";

	public String getDelSql() {
		return delSql;
	}

	public void setDelSql(String delSql) {
		this.delSql = delSql;
	}

	@Override
	public void run() {
		super.run();
		mt.insertSql(delSql);
		FixOfflineAnalysisRun.control("-");
	}

	public static void main(String[] args) throws InstantiationException, IllegalAccessException,
			ClassNotFoundException, SQLException, InterruptedException {

	}
}
