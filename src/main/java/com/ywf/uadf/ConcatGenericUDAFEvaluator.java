package com.ywf.uadf;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.StringUtils;

/**
 * ClassName MyGenericUDAFEvaluator
 * 功能: TODO
 * 运行方式与参数: TODO
 * Author YangWeiFeng
 * Date 2019/11/27 18:46
 * Version 1.0
 **/
public class ConcatGenericUDAFEvaluator extends GenericUDAFEvaluator {
    static final Log LOG = LogFactory.getLog(ConcatGenericUDAFEvaluator.class.getName());
    //Mode的各部分的输入都是String类型，输出也是，所以对应的OI实例也都一样
    PrimitiveObjectInspector inputOI;
    Text partialResult;
    Text result;

    @Override
    public ObjectInspector init(Mode mode, ObjectInspector[] parameters) throws HiveException {
        assert (parameters.length == 1);
        super.init(mode, parameters);
        // init input
        inputOI = (PrimitiveObjectInspector) parameters[0];
        // init output
        result = new Text("");
        return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    }

    /*
    PARTIAL1：原始数据到部分聚合，调用iterate和terminatePartial --> map阶段
    PARTIAL2: 部分聚合到部分聚合，调用merge和terminatePartial --> combine阶段
    FINAL: 部分聚合到完全聚合，调用merge和terminate --> reduce阶段
    COMPLETE: 从原始数据直接到完全聚合 --> map阶段，并且没有reduce
     */
    boolean warned = false;
    /**
     * 1 处理一行数据
     * @param agg
     * @param parameters
     * @throws HiveException
     */
    @Override
    public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
        Object p = parameters[0];
        if (p != null) {
            ConcatAgg myagg = (ConcatAgg) agg;
            try {
                String v = PrimitiveObjectInspectorUtils.getString(p, inputOI);
                if (myagg.line.length() == 0)
                    myagg.line.append(v);
                else
                    myagg.line.append("," + v);
            } catch (RuntimeException e) {
                if (!warned) {
                    warned = true;
                    LOG.warn(getClass().getSimpleName() + " "
                            + StringUtils.stringifyException(e));
                    LOG.warn(getClass().getSimpleName()
                            + " ignoring similar exceptions.");
                }
            }
        }
    }

    /**
     * 返回部分聚合数据的持久化对象。因为调用这个方法时，说明已经是map或者combine的结束了，必须将数据持久化以后交给reduce进行处理。
     * 只支持JAVA原始数据类型及其封装类型、HADOOP Writable类型、List、Map，不能返回自定义的类，即使实现了Serializable也不行，否则会出现问题或者错误的结果。
     * @param agg
     * @return
     * @throws HiveException
     */
    @Override
    public Object terminatePartial(AggregationBuffer agg) throws HiveException {
        ConcatAgg myagg = (ConcatAgg) agg;
        result.set(myagg.line.toString());
        return result;
    }

    /**
     * 结束，生成最终结果。
     * @param agg
     * @return
     * @throws HiveException
     */
    @Override
    public Object terminate(AggregationBuffer agg) throws HiveException {
        ConcatAgg myagg = (ConcatAgg) agg;
        result.set(myagg.line.toString());
        return result;
    }

    /**
     * 将terminatePartial返回的部分聚合数据进行合并，需要使用到对应的OI。
     * @param agg
     * @param partial
     * @throws HiveException
     */
    @Override
    public void merge(AggregationBuffer agg, Object partial) throws HiveException {
        if (partial != null) {
            try {
                ConcatAgg myagg = (ConcatAgg) agg;
                String v = PrimitiveObjectInspectorUtils.getString(partial, inputOI);
                if (myagg.line.length() == 0)
                    myagg.line.append(v);
                else
                    myagg.line.append("," + v);
            } catch (RuntimeException e) {
                if (!warned) {
                    warned = true;
                    LOG.warn(getClass().getSimpleName() + " "
                            + StringUtils.stringifyException(e));
                    LOG.warn(getClass().getSimpleName()
                            + " ignoring similar exceptions.");
                }
            }
        }
    }


    static class ConcatAgg implements AggregationBuffer {
        StringBuilder line = new StringBuilder("");
    }
    /**
     * 获取存放中间结果的对象
     * @return
     * @throws HiveException
     */
    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
        ConcatAgg result = new ConcatAgg();
        reset(result);
        return result;
    }

    /**
     * 重置
     * @param agg
     * @throws HiveException
     */
    @Override
    public void reset(AggregationBuffer agg) throws HiveException {
        ConcatAgg myagg = (ConcatAgg) agg;
        myagg.line.delete(0, myagg.line.length());
    }

}
