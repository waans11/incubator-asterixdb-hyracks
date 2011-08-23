/*
 * Copyright 2009-2010 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.hyracks.dataflow.std.group;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;

public class PreclusteredGroupWriter implements IFrameWriter {
    private final int[] groupFields;
    private final IBinaryComparator[] comparators;
    private final IAccumulatingAggregator aggregator;
    private final IFrameWriter writer;
    private final ByteBuffer copyFrame;
    private final FrameTupleAccessor inFrameAccessor;
    private final FrameTupleAccessor copyFrameAccessor;
    private final ByteBuffer outFrame;
    private final FrameTupleAppender appender;
    private boolean first;

    public PreclusteredGroupWriter(IHyracksTaskContext ctx, int[] groupFields, IBinaryComparator[] comparators,
            IAccumulatingAggregator aggregator, RecordDescriptor inRecordDesc, IFrameWriter writer) {
        this.groupFields = groupFields;
        this.comparators = comparators;
        this.aggregator = aggregator;
        this.writer = writer;
        copyFrame = ctx.allocateFrame();
        inFrameAccessor = new FrameTupleAccessor(ctx.getFrameSize(), inRecordDesc);
        copyFrameAccessor = new FrameTupleAccessor(ctx.getFrameSize(), inRecordDesc);
        copyFrameAccessor.reset(copyFrame);
        outFrame = ctx.allocateFrame();
        appender = new FrameTupleAppender(ctx.getFrameSize());
        appender.reset(outFrame, true);
    }

    @Override
    public void open() throws HyracksDataException {
        writer.open();
        first = true;
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        inFrameAccessor.reset(buffer);
        int nTuples = inFrameAccessor.getTupleCount();
        for (int i = 0; i < nTuples; ++i) {
            if (first) {
                aggregator.init(inFrameAccessor, i);
                first = false;
            } else {
                if (i == 0) {
                    switchGroupIfRequired(copyFrameAccessor, copyFrameAccessor.getTupleCount() - 1, inFrameAccessor, i);
                } else {
                    switchGroupIfRequired(inFrameAccessor, i - 1, inFrameAccessor, i);
                }
            }
            aggregator.accumulate(inFrameAccessor, i);
        }
        FrameUtils.copy(buffer, copyFrame);
    }

    private void switchGroupIfRequired(FrameTupleAccessor prevTupleAccessor, int prevTupleIndex,
            FrameTupleAccessor currTupleAccessor, int currTupleIndex) throws HyracksDataException {
        if (!sameGroup(prevTupleAccessor, prevTupleIndex, currTupleAccessor, currTupleIndex)) {
            writeOutput(prevTupleAccessor, prevTupleIndex);
            aggregator.init(currTupleAccessor, currTupleIndex);
        }
    }

    private void writeOutput(final FrameTupleAccessor lastTupleAccessor, int lastTupleIndex)
            throws HyracksDataException {
        if (!aggregator.output(appender, lastTupleAccessor, lastTupleIndex, groupFields)) {
            FrameUtils.flushFrame(appender.getBuffer(), writer);
            appender.reset(appender.getBuffer(), true);
            if (!aggregator.output(appender, lastTupleAccessor, lastTupleIndex, groupFields)) {
                throw new IllegalStateException();
            }
        }
    }

    private boolean sameGroup(FrameTupleAccessor a1, int t1Idx, FrameTupleAccessor a2, int t2Idx) {
        for (int i = 0; i < comparators.length; ++i) {
            int fIdx = groupFields[i];
            int s1 = a1.getTupleStartOffset(t1Idx) + a1.getFieldSlotsLength() + a1.getFieldStartOffset(t1Idx, fIdx);
            int l1 = a1.getFieldLength(t1Idx, fIdx);
            int s2 = a2.getTupleStartOffset(t2Idx) + a2.getFieldSlotsLength() + a2.getFieldStartOffset(t2Idx, fIdx);
            int l2 = a2.getFieldLength(t2Idx, fIdx);
            if (comparators[i].compare(a1.getBuffer().array(), s1, l1, a2.getBuffer().array(), s2, l2) != 0) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void fail() throws HyracksDataException {
        writer.fail();
    }

    @Override
    public void close() throws HyracksDataException {
        if (!first) {
            writeOutput(copyFrameAccessor, copyFrameAccessor.getTupleCount() - 1);
            if (appender.getTupleCount() > 0) {
                FrameUtils.flushFrame(appender.getBuffer(), writer);
            }
        }
        writer.close();
    }
}