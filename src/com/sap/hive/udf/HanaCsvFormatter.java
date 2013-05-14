package com.sap.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;

@UDFType(deterministic = true)
//@formatter:off
@Description(
        name = "to_hana_csv",
        value = "_FUNC_(input) - transform the select clause into HANA " +
                " SQL-Script acceptable CSV format (every value is enclosed " +
                " by double quote: '\"' ",
        extended = " Convert the select clause into HANA acceptable CSV format."
)
//@formatter:on
public class HanaCsvFormatter extends GenericUDF {

    /**
     * A placeholder for result.
     */
    private Text result = new Text();

    /**
     * Converters for retrieving the arguments to the UDF.
     */
    private ObjectInspectorConverters.Converter[] converters;

    //private boolean[] isStringArray;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        for (int i = 0; i < arguments.length; i++) {
            if (arguments[i].getCategory() != ObjectInspector.Category.PRIMITIVE) {
                throw new UDFArgumentTypeException(i,
                        "Only primitive argument was expected but an argument of type " + arguments[i].getTypeName()
                                + " was given at position " + (i + 1));

            }
        }
        converters = new ObjectInspectorConverters.Converter[arguments.length];
        //isStringArray = new boolean[arguments.length];
        for (int i = 0; i < arguments.length; i++) {
            /*
            // If a particular column is already a string, we mark it and don't
            // add double quotes later on
            isStringArray[i] =
                    ((PrimitiveObjectInspector) arguments[0]).getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.STRING;
            */
            converters[i] = ObjectInspectorConverters.getConverter(arguments[i],
                    PrimitiveObjectInspectorFactory.writableStringObjectInspector);
        }

        // We will be returning a Text object
        return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    }

    private boolean isWrapped(String input, char c) {
        return input != null && input.length() > 1 && input.charAt(0) == c && input.charAt(input.length() - 1) == c;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {

        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < arguments.length; i++) {
            String value = converters[i].convert(arguments[i].get()).toString();
            boolean isWrapped = isWrapped(value, '\"');
            if (!isWrapped) {
                sb.append("\"");
            }
            sb.append(value);
            if (!isWrapped) {
                sb.append("\"");
            }

            if (i < (arguments.length - 1)) {
                sb.append(",");
            }
        }

        result.set(sb.toString());
        return result;
    }

    @Override
    public String getDisplayString(String[] children) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < children.length; i++) {
            sb.append("translate(");
            sb.append(children[i]);
            sb.append(", ");
        }
        sb.delete(sb.length() - 2, sb.length() - 1);
        sb.append(")");
        return sb.toString();
    }
}
