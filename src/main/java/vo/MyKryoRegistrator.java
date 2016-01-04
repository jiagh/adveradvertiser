package vo;

import org.apache.spark.serializer.KryoRegistrator;
import com.esotericsoftware.kryo.Kryo;
import handle.report.ReportOut;
import handle.report.admin.Admin_All_Advertisers_Cost_Report;
import handle.report.admin.Admin_All_Day_Report;
import handle.report.admin.Admin_All_Project_Report;
import handle.report.advertisers.Advertisers_Area_Report;
import handle.report.advertisers.Advertisers_Project_Report;
import handle.report.publisher.Publisher_Creative_Report;
import handle.report.publisher.Publisher_Income_Report;
import handle.report.publisher.Publisher_Slot_Report;
import offline.OfflineAnalysisRun;
import spark.process.OrganizaInsertSql;
import spark.process.ReportKeyValueGenerate;
import spark.process.ReportKeyValueOutPut;
import spark.process.ReportSplitKeyValue;
import spark.process.ReportValueMerge;
import vo.report.key.admin.Admin_All_Advertisers_Cost_Report_Key;
import vo.report.key.admin.Admin_All_Day_Report_Key;
import vo.report.key.admin.Admin_All_Project_Report_Key;
import vo.report.key.advertisers.Advertisers_Area_Report_Key;
import vo.report.key.advertisers.Advertisers_Project_Report_Key;
import vo.report.key.publisher.Publisher_Creative_Report_Key;
import vo.report.key.publisher.Publisher_Income_Report_Key;
import vo.report.key.publisher.Publisher_Slot_Report_Key;
import vo.report.value.publicReportValue;
import vo.report.value.admin.Admin_All_Advertisers_Cost_Report_Value;
import vo.report.value.admin.Admin_All_Day_Report_Value;
import vo.report.value.admin.Admin_All_Project_Report_Value;
import vo.report.value.advertisers.Advertisers_Area_Report_Value;
import vo.report.value.advertisers.Advertisers_Project_Report_Value;
import vo.report.value.publisher.Publisher_Creative_Report_Value;
import vo.report.value.publisher.Publisher_Income_Report_Value;
import vo.report.value.publisher.Publisher_Slot_Report_Value;

public class MyKryoRegistrator implements KryoRegistrator {

	@Override
	public void registerClasses(Kryo arg0) {

		arg0.register(Admin_All_Advertisers_Cost_Report.class);
		arg0.register(Admin_All_Day_Report.class);
		arg0.register(Admin_All_Project_Report.class);
		arg0.register(Advertisers_Area_Report.class);
		arg0.register(Advertisers_Project_Report.class);
		arg0.register(Publisher_Creative_Report.class);
		arg0.register(Publisher_Income_Report.class);
		arg0.register(Publisher_Slot_Report.class);
		arg0.register(OfflineAnalysisRun.class);
		arg0.register(ReportValueMerge.class);
		arg0.register(ReportKeyValueOutPut.class);
		arg0.register(OrganizaInsertSql.class);
		arg0.register(ReportKeyValueGenerate.class);
		arg0.register(ReportSplitKeyValue.class);
		arg0.register(Admin_All_Advertisers_Cost_Report_Key.class);
		arg0.register(Admin_All_Day_Report_Key.class);
		arg0.register(Admin_All_Project_Report_Key.class);
		arg0.register(Advertisers_Area_Report_Key.class);
		arg0.register(Advertisers_Project_Report_Key.class);
		arg0.register(Publisher_Creative_Report_Key.class);
		arg0.register(Publisher_Income_Report_Key.class);
		arg0.register(Publisher_Slot_Report_Key.class);
		arg0.register(publicReportValue.class);
		arg0.register(Admin_All_Advertisers_Cost_Report_Value.class);
		arg0.register(Admin_All_Day_Report_Value.class);
		arg0.register(Admin_All_Project_Report_Value.class);
		arg0.register(Advertisers_Area_Report_Value.class);
		arg0.register(Advertisers_Project_Report_Value.class);
		arg0.register(Publisher_Creative_Report_Value.class);
		arg0.register(Publisher_Income_Report_Value.class);
		arg0.register(Publisher_Slot_Report_Value.class);
		arg0.register(Admin_All_Day_Report_Value.class);
		arg0.register(ReportOut.class);

	}

}
