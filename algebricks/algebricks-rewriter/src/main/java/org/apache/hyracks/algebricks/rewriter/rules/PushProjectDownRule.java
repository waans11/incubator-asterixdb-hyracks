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
package org.apache.hyracks.algebricks.rewriter.rules;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractOperatorWithNestedPlans;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.GroupByOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ProjectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

/**
 * Pushes projections through its input operator, provided that operator does
 * not produce the projected variables.
 *
 * @author Nicola
 */
public class PushProjectDownRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context) {
        return false;
    }

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) throws AlgebricksException {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        if (op.getOperatorTag() != LogicalOperatorTag.PROJECT) {
            return false;
        }
        ProjectOperator projectOp = (ProjectOperator) op;
        Mutable<ILogicalOperator> childOpRef = projectOp.getInputs().get(0);

        HashSet<LogicalVariable> variablesToPush = new HashSet<LogicalVariable>();
        variablesToPush.addAll(projectOp.getVariables());

        Pair<Boolean, Boolean> p = pushThroughOp(variablesToPush, childOpRef, op, context);
        boolean smthWasPushed = p.first;
        if (p.second) { // the original projection is redundant
            opRef.setValue(op.getInputs().get(0).getValue());
            smthWasPushed = true;
        }

        return smthWasPushed;
    }

    private static Pair<Boolean, Boolean> pushThroughOp(HashSet<LogicalVariable> variablesToPush,
            Mutable<ILogicalOperator> childOpRef, ILogicalOperator initialOp, IOptimizationContext context)
            throws AlgebricksException {
        List<LogicalVariable> initialProjectList = new ArrayList<LogicalVariable>(variablesToPush);
        AbstractLogicalOperator childOp = (AbstractLogicalOperator) childOpRef.getValue();
        do {
            if (childOp.getOperatorTag() == LogicalOperatorTag.EMPTYTUPLESOURCE
                    || childOp.getOperatorTag() == LogicalOperatorTag.NESTEDTUPLESOURCE
                    || childOp.getOperatorTag() == LogicalOperatorTag.PROJECT
                    || childOp.getOperatorTag() == LogicalOperatorTag.REPLICATE
                    || childOp.getOperatorTag() == LogicalOperatorTag.SPLIT
                    || childOp.getOperatorTag() == LogicalOperatorTag.UNIONALL) {
                return new Pair<Boolean, Boolean>(false, false);
            }
            if (!childOp.isMap()) {
                break;
            }
            LinkedList<LogicalVariable> childOpUsedVars = new LinkedList<LogicalVariable>();
            VariableUtilities.getUsedVariables(childOp, childOpUsedVars);
            variablesToPush.addAll(childOpUsedVars);

            LinkedList<LogicalVariable> childOpProducedVars = new LinkedList<LogicalVariable>();
            VariableUtilities.getProducedVariables(childOp, childOpProducedVars);
            variablesToPush.removeAll(childOpProducedVars);

            // We assume pipeline-able ops that has only one input.
            childOpRef = childOp.getInputs().get(0);
            childOp = (AbstractLogicalOperator) childOpRef.getValue();
        } while (true);

        LinkedList<LogicalVariable> producedVars2 = new LinkedList<LogicalVariable>();
        VariableUtilities.getProducedVariables(childOp, producedVars2);
        LinkedList<LogicalVariable> usedVars2 = new LinkedList<LogicalVariable>();
        VariableUtilities.getUsedVariables(childOp, usedVars2);

        // If true, we can get rid of the initial projection.
        boolean canCommuteProjection = initialProjectList.containsAll(variablesToPush)
                && initialProjectList.containsAll(producedVars2) && initialProjectList.containsAll(usedVars2);

        // Get rid of useless decor variables in the GROUP-BY operator.
        if (!canCommuteProjection && childOp.getOperatorTag() == LogicalOperatorTag.GROUP) {
            boolean gbyChanged = false;
            GroupByOperator gbyOp = (GroupByOperator) childOp;
            List<Pair<LogicalVariable, Mutable<ILogicalExpression>>> newDecorList = new ArrayList<Pair<LogicalVariable, Mutable<ILogicalExpression>>>();
            for (Pair<LogicalVariable, Mutable<ILogicalExpression>> p : gbyOp.getDecorList()) {
                LogicalVariable decorVar = GroupByOperator.getDecorVariable(p);
                if (!variablesToPush.contains(decorVar)) {
                    usedVars2.remove(decorVar);
                    gbyChanged = true;
                } else {
                    newDecorList.add(p);
                }
            }
            gbyOp.getDecorList().clear();
            gbyOp.getDecorList().addAll(newDecorList);
            if (gbyChanged) {
                context.computeAndSetTypeEnvironmentForOperator(gbyOp);
            }
        }
        usedVars2.clear();
        VariableUtilities.getUsedVariables(childOp, usedVars2);

        variablesToPush.addAll(usedVars2); // remember that toPush is a Set
        variablesToPush.removeAll(producedVars2);

        if (variablesToPush.isEmpty()) {
            return new Pair<Boolean, Boolean>(false, false);
        }

        boolean smthWasPushed = false;
        for (Mutable<ILogicalOperator> c : childOp.getInputs()) {
            if (pushNeededProjections(variablesToPush, c, context, initialOp)) {
                smthWasPushed = true;
            }
        }
        if (childOp.hasNestedPlans()) {
            AbstractOperatorWithNestedPlans n = (AbstractOperatorWithNestedPlans) childOp;
            for (ILogicalPlan p : n.getNestedPlans()) {
                for (Mutable<ILogicalOperator> r : p.getRoots()) {
                    if (pushNeededProjections(variablesToPush, r, context, initialOp)) {
                        smthWasPushed = true;
                    }
                }
            }
        }
        return new Pair<Boolean, Boolean>(smthWasPushed, canCommuteProjection);
    }

    // It does not try to push above another Projection.
    private static boolean pushNeededProjections(HashSet<LogicalVariable> toPush, Mutable<ILogicalOperator> opRef,
            IOptimizationContext context, ILogicalOperator initialOp) throws AlgebricksException {
        HashSet<LogicalVariable> allP = new HashSet<LogicalVariable>();
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        VariableUtilities.getLiveVariables(op, allP);

        HashSet<LogicalVariable> toProject = new HashSet<LogicalVariable>();
        for (LogicalVariable v : toPush) {
            if (allP.contains(v)) {
                toProject.add(v);
            }
        }
        if (toProject.equals(allP)) {
            // projection would be redundant, since we would project everything
            // but we can try with the children
            boolean push = false;
            if (pushThroughOp(toProject, opRef, initialOp, context).first) {
                push = true;
            }
            return push;
        } else {
            return pushAllProjectionsOnTopOf(toProject, opRef, context, initialOp);
        }
    }

    // It does not try to push above another Projection.
    private static boolean pushAllProjectionsOnTopOf(Collection<LogicalVariable> toPush,
            Mutable<ILogicalOperator> opRef, IOptimizationContext context, ILogicalOperator initialOp)
            throws AlgebricksException {
        if (toPush.isEmpty()) {
            return false;
        }
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();

        if (context.checkAndAddToAlreadyCompared(initialOp, op)) {
            return false;
        }

        switch (op.getOperatorTag()) {
            case EXCHANGE: {
                opRef = opRef.getValue().getInputs().get(0);
                op = (AbstractLogicalOperator) opRef.getValue();
                break;
            }
            case PROJECT: {
                return false;
            }
        }

        ProjectOperator pi2 = new ProjectOperator(new ArrayList<LogicalVariable>(toPush));
        pi2.getInputs().add(new MutableObject<ILogicalOperator>(op));
        opRef.setValue(pi2);
        pi2.setExecutionMode(op.getExecutionMode());
        context.computeAndSetTypeEnvironmentForOperator(pi2);
        return true;
    }

}
