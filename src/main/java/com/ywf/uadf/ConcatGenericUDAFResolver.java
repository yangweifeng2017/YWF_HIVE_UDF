package com.ywf.uadf;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

/**
 * ClassName MyGenericUDAFResolver
 * 功能: TODO
 * 运行方式与参数: TODO
 * Author YangWeiFeng
 * Date 2019/11/27 18:45
 * Version 1.0
 **/
public class ConcatGenericUDAFResolver extends AbstractGenericUDAFResolver {
    static final Log LOG = LogFactory.getLog(ConcatGenericUDAFResolver.class.getName());

    @Override
    public GenericUDAFEvaluator getEvaluator(GenericUDAFParameterInfo info) throws SemanticException {
        // TODO: 1. Type-checking goes here!
        return new ConcatGenericUDAFEvaluator();
    }

    /**
     * 校验UDAF的入参个数和入参类型并返回Evaluator对象。调用者传入不同的参数时，向其返回不同的Evaluator或者直接抛出异常。这部分代
     * 码可以写入骨架代码中的TODO：1处。例如本例中的实现，该UDAF不支持多种参数的版本，限定参数个数必须为2，并且第一个参数必须是简单数据类型，第二个参数必须是int。
     * @param parameters
     * @return
     * @throws SemanticException
     */
    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {
        if (parameters.length != 1) {
            throw new UDFArgumentTypeException(parameters.length - 1,
                    "Please specify exactly two arguments.");
        }
        // validate the first parameter, which is the expression to compute over
        if (parameters[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentTypeException(0,
                    "Only primitive type arguments are accepted but "
                            + parameters[0].getTypeName() + " was passed as parameter 1.");
        }
        switch (((PrimitiveTypeInfo) parameters[0]).getPrimitiveCategory()) {
            case BYTE:
            case SHORT:
            case INT:
            case LONG:
            case FLOAT:
            case DOUBLE:
            case TIMESTAMP:
            case DECIMAL:
                break;
            case STRING:
            case BOOLEAN:
            case DATE:
            default:
                throw new UDFArgumentTypeException(0,
                        "Only numeric type arguments are accepted but "
                                + parameters[0].getTypeName() + " was passed as parameter 1.");
        }

        /*
        // validate the second parameter, which is the number of histogram bins
        if (parameters[1].getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentTypeException(1,
                    "Only primitive type arguments are accepted but "
                            + parameters[1].getTypeName() + " was passed as parameter 2.");
        }
        if( ((PrimitiveTypeInfo) parameters[1]).getPrimitiveCategory()
                != PrimitiveObjectInspector.PrimitiveCategory.INT) {
            throw new UDFArgumentTypeException(1,
                    "Only an integer argument is accepted as parameter 2, but "
                            + parameters[1].getTypeName() + " was passed instead.");
        }
        */
        return new ConcatGenericUDAFEvaluator();

    }
}
