
package openkb.drill.udf;

import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.FunctionScope;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.NullHandling;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.holders.BigIntHolder;
import org.apache.drill.exec.expr.holders.VarCharHolder;
import org.apache.drill.exec.record.RecordBatch;


@FunctionTemplate(name = "to_int", scope = FunctionScope.SIMPLE, nulls = NullHandling.INTERNAL)
public class GetInt2 implements DrillSimpleFunc
{

    @Param
    VarCharHolder input;

    @Param
    BigIntHolder defaultVal;

    @Output
    BigIntHolder out;

    public void eval()
    {
        try {
                out.value = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.varCharToLong(input.start, input.end, input.buffer);
             } catch (Exception e)
             {
                out.value = defaultVal.value;
             }
    }

    @Override
    public void setup()
    {
    }


}
