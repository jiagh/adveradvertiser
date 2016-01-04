package handle.report;

import java.util.ArrayList;

import scala.Tuple2;
import vo.log1_2.ReqLog1_2;

public abstract class AbstractHandleReport {

	public abstract ArrayList<Tuple2<String, String>> outMap(ReqLog1_2 rl, Object... objects) throws Exception;

	public abstract String outReduce(String key, String a0, String a1, Object... objects) throws Exception;

}
