package com.yuqi.engine.data.func;

import com.yuqi.engine.data.type.DataType;
import com.yuqi.engine.data.value.Value;

import java.util.List;

import static com.yuqi.engine.data.type.DataTypes.BOOLEAN;

/**
 * @author yuqi
 * @mail yuqi4733@gmail.com
 * @description your description
 * @time 12/8/20 15:19
 **/
public class CaseFunction extends Scalar {

    public static final CaseFunction INSTANCE = new CaseFunction();

    public CaseFunction() {
        super(3);
    }

    @Override
    public Value evaluate(List<Value> args, DataType returnType) {
        final Boolean booleanValue = args.get(0).booleanValue();
        return new Value(booleanValue ? args.get(1) : args.get(2), returnType);
    }
}

