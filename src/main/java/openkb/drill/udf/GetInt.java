
package openkb.drill.udf;

import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.FunctionScope;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.NullHandling;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.holders.BigIntHolder;
import org.apache.drill.exec.expr.holders.VarCharHolder;
import org.apache.drill.exec.expr.holders.NullableVarCharHolder;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.log4j.Logger;


@FunctionTemplate(name = "to_int", scope = FunctionScope.SIMPLE, nulls = NullHandling.INTERNAL)
public class GetInt implements DrillSimpleFunc
{

    @Param
    NullableVarCharHolder input;

    @Param
    BigIntHolder defaultVal;

    @Output
    BigIntHolder out;

    public void eval()
    {
        if (input.isSet == 0)
        {
            out.value = defaultVal.value;
        } else
        {   try {
                   out.value = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.varCharToLong(input.start, input.end, input.buffer);
                } catch (Exception e)
                {
                   out.value = defaultVal.value;
                }
        }
    }

    @Override
    public void setup()
    {
    }


}
