package org.tao_chen.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BooleanWritable;
import org.tao_chen.hive.utils.OtgController;

import java.util.ArrayList;
import java.util.List;

/**
 * @author tao_chen
 * date: 2020/6/4
 * time: 10:32
 * description:
 */

@Description(name = "is_in_polygon",
        value = "_FUNC_(desLongitude, desLatitude, longitudeArray, LatitudeArray) - return true if des point in polygon, else false")
public class UDFIsInPolygon extends GenericUDF {
    private static final int ARG_COUNT = 4;
    private transient ListObjectInspector arrayOI;
    private transient ObjectInspector arrayElementOI;
    private transient DoubleObjectInspector doubleOI;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        // Check if 4 arguments were passed
        if (arguments.length != ARG_COUNT) {
            throw new UDFArgumentLengthException(
                    "The function is_in_polygon(desLongitude, desLatitude, longitudeArray, LatitudeArray) takes exactly " + ARG_COUNT + " arguments.");
        }

        doubleOI = (DoubleObjectInspector) arguments[0];
        arrayOI = (ListObjectInspector) arguments[2];

        arrayElementOI = arrayOI.getListElementObjectInspector();

        return PrimitiveObjectInspectorFactory.writableBooleanObjectInspector;
        }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
//        arrayOI = ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);
//        doubleOI = PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
        Object argument0 = arguments[0].get();
        Object argument1 = arguments[1].get();
        Object argument2 = arguments[2].get();
        Object argument3 = arguments[3].get();

        double desLongitude = doubleOI.get(argument0);
        double desLatitude = doubleOI.get(argument1);
        List<?> longitudeList = arrayOI.getList(argument2);
        ArrayList<Double> polygonLongitude = new ArrayList<>();
        for (Object element : longitudeList) {
            polygonLongitude.add(Double.valueOf(String.valueOf(element)));
        }

        List<?> latitudeList = arrayOI.getList(argument3);
        ArrayList<Double> polygonLatitude = new ArrayList<>();
        for (Object element : latitudeList) {
            polygonLatitude.add(Double.valueOf(String.valueOf(element)));
        }

        OtgController otg = new OtgController();

        boolean res = otg.inPointInPolygon(desLongitude, desLatitude, polygonLongitude, polygonLatitude);
        return new BooleanWritable(res);
    }

    @Override
    public String getDisplayString(String[] children) {
        return null;
    }
}
