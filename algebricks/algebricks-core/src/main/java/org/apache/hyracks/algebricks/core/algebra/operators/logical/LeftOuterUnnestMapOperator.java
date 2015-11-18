/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hyracks.algebricks.core.algebra.operators.logical;

import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.properties.VariablePropagationPolicy;
import org.apache.hyracks.algebricks.core.algebra.typing.ITypingContext;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalOperatorVisitor;

/**
 * Left-outer-unnest-map is similar to unnest-map operator.
 * The only difference is this represents left-outer semantics, meaning that
 * it generates null values for non-matching tuples. It also propagates all input variables.
 * This may be used only in a left-outer-join case.
 */
public class LeftOuterUnnestMapOperator extends AbstractUnnestOperator {

    private final List<Object> variableTypes;

    private List<Mutable<ILogicalExpression>> additionalFilteringExpressions;
    private List<LogicalVariable> minFilterVars;
    private List<LogicalVariable> maxFilterVars;

    public LeftOuterUnnestMapOperator(List<LogicalVariable> variables, Mutable<ILogicalExpression> expression,
            List<Object> variableTypes) {
        super(variables, expression);
        this.variableTypes = variableTypes;
    }

    @Override
    public LogicalOperatorTag getOperatorTag() {
        return LogicalOperatorTag.LEFT_OUTER_UNNEST_MAP;
    }

    @Override
    public <R, T> R accept(ILogicalOperatorVisitor<R, T> visitor, T arg) throws AlgebricksException {
        return visitor.visitLeftOuterUnnestMapOperator(this, arg);
    }

    // LeftOuterUnnestMap propagates all input variables.
    @Override
    public VariablePropagationPolicy getVariablePropagationPolicy() {
        return new VariablePropagationPolicy() {
            @Override
            public void propagateVariables(IOperatorSchema target, IOperatorSchema... sources)
                    throws AlgebricksException {
                target.addAllVariables(sources[0]);
                for (LogicalVariable v : variables) {
                    target.addVariable(v);
                }
            }
        };
    }

    public List<Object> getVariableTypes() {
        return variableTypes;
    }

    @Override
    public IVariableTypeEnvironment computeOutputTypeEnvironment(ITypingContext ctx) throws AlgebricksException {
        IVariableTypeEnvironment env = null;
        env = createPropagatingAllInputsTypeEnvironment(ctx);
        int n = variables.size();
        for (int i = 0; i < n; i++) {
            env.setVarType(variables.get(i), variableTypes.get(i));
        }
        return env;
    }

    public boolean propagatesInput() {
        // This method exists to be compatible with unnest-map.
        return true;
    }

    public List<LogicalVariable> getMinFilterVars() {
        return minFilterVars;
    }

    public void setMinFilterVars(List<LogicalVariable> minFilterVars) {
        this.minFilterVars = minFilterVars;
    }

    public List<LogicalVariable> getMaxFilterVars() {
        return maxFilterVars;
    }

    public void setMaxFilterVars(List<LogicalVariable> maxFilterVars) {
        this.maxFilterVars = maxFilterVars;
    }

    public void setAdditionalFilteringExpressions(List<Mutable<ILogicalExpression>> additionalFilteringExpressions) {
        this.additionalFilteringExpressions = additionalFilteringExpressions;
    }

    public List<Mutable<ILogicalExpression>> getAdditionalFilteringExpressions() {
        return additionalFilteringExpressions;
    }

    /*
    @Override
    public boolean isMap() {
        return !propagateInput;
    }
    */
}