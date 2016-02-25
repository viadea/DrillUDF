package openkb.drill.udf;

import io.netty.buffer.DrillBuf;
import javax.inject.Inject;
import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.FunctionScope;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.NullHandling;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.holders.IntHolder;
import org.apache.drill.exec.expr.holders.VarCharHolder;
import org.apache.drill.exec.record.RecordBatch;


@FunctionTemplate(name = "dup_string", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
public class DupString implements DrillSimpleFunc
{

    @Param
    VarCharHolder input_vc;

    @Param
    IntHolder num_times;

    @Output
    VarCharHolder out;

    @Inject
    DrillBuf buffer;

    public void eval()
    {
         String input_str = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(input_vc.start, input_vc.end, input_vc.buffer);
         String output_str = openkb.drill.udf.DupString.repeat_strng(input_str,num_times.value); 

         out.buffer = buffer;
         out.start = 0;
         out.end = output_str.getBytes().length;
         buffer.setBytes(0, output_str.getBytes());
    }

    @Override
    public void setup()
    {
    }


    public static String repeat_strng(String s, int n)
    {
       return new String(new char[n]).replace("\0", s);
    }

}
