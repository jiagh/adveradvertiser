package hive.udf;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;

public class SortArrayTimeStampReturnUrlArrayUdf extends GenericUDF {

	ObjectInspectorConverters.Converter converter;

	private ListObjectInspector listOI = null;
	private ObjectInspector elementOI = null;

	@Override
	public Object evaluate(DeferredObject[] arg0) throws HiveException {

		List<String> inputs = (List<String>) ObjectInspectorUtils.copyToStandardObject(arg0[0].get(), listOI);

		Collections.sort(inputs, new Comparator<String>() {
			@Override
			public int compare(String o1, String o2) {
				String tmp1[] = o1.toString().split("\\|-\\|");
				String tmp2[] = o2.toString().split("\\|-\\|");
				return Long.parseLong(tmp1[0]) > Long.parseLong(tmp2[0]) ? 1 : -1;
			}
		});
		return inputs;
	}

	@Override
	public String getDisplayString(String[] arg0) {
		return null;
	}

	@Override
	public ObjectInspector initialize(ObjectInspector[] arg0) throws UDFArgumentException {

		listOI = (ListObjectInspector) arg0[0];
		elementOI = ObjectInspectorUtils.getStandardObjectInspector(listOI.getListElementObjectInspector());
		return elementOI;

	}
}
