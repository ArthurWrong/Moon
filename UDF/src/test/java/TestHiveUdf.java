import junit.framework.TestCase;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableBooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableDoubleObjectInspector;
import org.apache.hadoop.io.DoubleWritable;
import org.junit.Assert;
import org.tao_chen.hive.udf.UDFIsInPolygon;

import static java.util.Arrays.asList;

/**
 * @author tao_chen
 * date: 2020/6/16
 * time: 11:34
 * description:
 */

public class TestHiveUdf extends TestCase {

    public void testIsInPolygon() throws HiveException {
        UDFIsInPolygon udf = new UDFIsInPolygon();
        StandardListObjectInspector arrayOI = ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);
        WritableDoubleObjectInspector doubleOI = PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
        WritableBooleanObjectInspector resOI = PrimitiveObjectInspectorFactory.writableBooleanObjectInspector;

        ObjectInspector[] arguments = {doubleOI, doubleOI, arrayOI, arrayOI};
        udf.initialize(arguments);
        GenericUDF.DeferredJavaObject valueObj1 = new GenericUDF.DeferredJavaObject(new DoubleWritable(113.9079236));
        GenericUDF.DeferredJavaObject valueObj2 = new GenericUDF.DeferredJavaObject(new DoubleWritable(22.3075597));
        GenericUDF.DeferredJavaObject valueObj3 = new GenericUDF.DeferredJavaObject(asList(new DoubleWritable(113.897051), new DoubleWritable(113.897051), new DoubleWritable(113.94599), new DoubleWritable(113.9459)));
        GenericUDF.DeferredJavaObject valueObj4 = new GenericUDF.DeferredJavaObject(asList(new DoubleWritable(22.301719), new DoubleWritable(22.32022), new DoubleWritable(22.32022), new DoubleWritable(22.301719)));
        GenericUDF.DeferredJavaObject[] args ={
                valueObj1, valueObj2, valueObj3, valueObj4
        };
        Object res = udf.evaluate(args);
        Assert.assertTrue(resOI.get(res));
    }

    public static void main(String[] args) throws HiveException {
        TestHiveUdf testHiveUdf = new TestHiveUdf();
        testHiveUdf.testIsInPolygon();
    }
}
