package org.tao_chen.hive.udtf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

import java.util.ArrayList;
import java.util.List;

/**
 * @author tao_chen
 * date: 2019/9/6
 * time: 14:47
 * description:
 */
@Description(name = "array_parallelization",
        value = "_FUNC_(arr1, arr2) - match element of each array with same index ")
public class UDTFArrayParallelization extends GenericUDTF {
    private ListObjectInspector listOI1 = null;
    private ListObjectInspector listOI2 = null;
    private final Object[] forwardObj = new Object[] { null, null };

    @Override
    public StructObjectInspector initialize(ObjectInspector[] inputOIs) throws UDFArgumentException {

        if (inputOIs.length != 2) {
            throw new UDFArgumentException("array_parallelization() takes exactly two argument");
        }

        if ((inputOIs[0].getCategory() != ObjectInspector.Category.LIST) || (inputOIs[1].getCategory() != ObjectInspector.Category.LIST)) {
            throw new UDFArgumentException("array_parallelization() takes two array as parameter");
        }

        listOI1 = (ListObjectInspector) inputOIs[0];
        listOI2 = (ListObjectInspector) inputOIs[1];

        ArrayList<String> fieldNames = new ArrayList<String>();
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
        fieldNames.add("col1");
        fieldNames.add("col2");
        fieldOIs.add(listOI1.getListElementObjectInspector());
        fieldOIs.add(listOI2.getListElementObjectInspector());
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    @Override
    public void process(Object[] args) throws HiveException {
        int maxSize = 0;
        List<?> list1 = listOI1.getList(args[0]);
        List<?> list2 = listOI2.getList(args[1]);

        if (list1 == null || list2 == null) {
            return;
        }

        if (list1.size() > list2.size()) {
            maxSize = list1.size();
        } else {
            maxSize = list2.size();
        }

        for (int i = 0; i < maxSize; i++) {
            try {
                forwardObj[0] = list1.get(i);
            } catch (Exception e) {
                forwardObj[0] = null;
            }
            try {
                forwardObj[1] = list2.get(i);
            } catch (Exception e) {
                forwardObj[1] = null;
            }
            forward(forwardObj);
        }
    }

    @Override
    public void close() throws HiveException {

    }
}
