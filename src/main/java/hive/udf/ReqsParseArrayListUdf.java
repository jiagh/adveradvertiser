package hive.udf;

import java.util.ArrayList;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

public class ReqsParseArrayListUdf extends GenericUDF {

	ObjectInspectorConverters.Converter converter;

	@Override
	public Object evaluate(DeferredObject[] arg0) throws HiveException {

		try {
			JsonArray ja = new JsonParser().parse(arg0[0].get().toString()).getAsJsonArray();
			ArrayList<Text> re = new ArrayList<Text>();
			for (JsonElement je : ja) {
				re.add(new Text(je.toString()));
			}
			return re;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public String getDisplayString(String[] arg0) {
		return null;
	}

	@Override
	public ObjectInspector initialize(ObjectInspector[] arg0) throws UDFArgumentException {

		converter = ObjectInspectorConverters.getConverter(arg0[0],
				PrimitiveObjectInspectorFactory.javaStringObjectInspector);

		return ObjectInspectorFactory
				.getStandardListObjectInspector((PrimitiveObjectInspectorFactory.writableStringObjectInspector));

	}
}
