/*
 * Copyright 2009-2013 by The Regents of the University of California
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
package edu.uci.ics.hyracks.dataflow.std.misc;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.comm.VSizeFrame;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.ActivityId;
import edu.uci.ics.hyracks.api.dataflow.IActivityGraphBuilder;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.TaskId;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.IOperatorDescriptorRegistry;
import edu.uci.ics.hyracks.data.std.util.GrowableArray;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractActivityNode;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputOperatorNodePushable;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;

public class SplitOperatorDescriptor extends AbstractOperatorDescriptor {
    private static final long serialVersionUID = 1L;

    private final static int SPLITTER_MATERIALIZER_ACTIVITY_ID = 0;
    private final static int MATERIALIZE_READER_ACTIVITY_ID = 1;

    private boolean[] outputMaterializationFlags;
    private boolean requiresMaterialization;
    private int numberOfNonMaterializedOutputs = 0;
    private int numberOfActiveMaterializeReaders = 0;
    // Whether this is a conditional SPLIT based on the last field value
    private boolean conditionalSplitRequired = false;
    private RecordDescriptor inputRecordDesc;
    private RecordDescriptor outputRecordDesc;

    public SplitOperatorDescriptor(IOperatorDescriptorRegistry spec, RecordDescriptor rDesc, int outputArity) {
        this(spec, rDesc, outputArity, new boolean[outputArity], false);
    }

    public SplitOperatorDescriptor(IOperatorDescriptorRegistry spec, RecordDescriptor rDesc, int outputArity,
            boolean[] outputMaterializationFlags) {
        this(spec, rDesc, outputArity, outputMaterializationFlags, false);
    }

    public SplitOperatorDescriptor(IOperatorDescriptorRegistry spec, RecordDescriptor rDesc, int outputArity,
            boolean[] outputMaterializationFlags, boolean conditionalSplitRequired) {
        super(spec, 1, outputArity);

        this.conditionalSplitRequired = conditionalSplitRequired;
		this.inputRecordDesc = rDesc;

    	// If conditional split is set, we should not propagate last field since it is
    	// used to determine which output it needs to go.
    	if (conditionalSplitRequired) {
    	    ISerializerDeserializer[] fields = new ISerializerDeserializer[rDesc.getFieldCount()-1];
    	    ITypeTraits[] typeTraits = new ITypeTraits[rDesc.getFieldCount()-1];

    	    for (int i = 0; i < rDesc.getFieldCount()-1; i++) {
    	    	fields[i] = rDesc.getFields()[i];
    	    	typeTraits[i] = rDesc.getTypeTraits()[i];
    	    }

    	    outputRecordDesc = new RecordDescriptor(fields, typeTraits);

            for (int i = 0; i < outputArity; i++) {
                recordDescriptors[i] = outputRecordDesc;
            }
    	} else {
            for (int i = 0; i < outputArity; i++) {
                recordDescriptors[i] = rDesc;
            }
    	}

        this.outputMaterializationFlags = outputMaterializationFlags;
        requiresMaterialization = false;
        for (boolean flag : outputMaterializationFlags) {
            if (flag) {
                requiresMaterialization = true;
                break;
            }
        }
    }

    @Override
    public void contributeActivities(IActivityGraphBuilder builder) {
        SplitterMaterializerActivityNode sma = new SplitterMaterializerActivityNode(new ActivityId(odId,
                SPLITTER_MATERIALIZER_ACTIVITY_ID));
        builder.addActivity(this, sma);
        builder.addSourceEdge(0, sma, 0);
        int taskOutputIndex = 0;
        for (int i = 0; i < outputArity; i++) {
            if (!outputMaterializationFlags[i]) {
                builder.addTargetEdge(i, sma, taskOutputIndex);
                taskOutputIndex++;
            }
        }
        numberOfNonMaterializedOutputs = taskOutputIndex;

        if (requiresMaterialization) {
            int activityId = MATERIALIZE_READER_ACTIVITY_ID;
            for (int i = 0; i < outputArity; i++) {
                if (outputMaterializationFlags[i]) {
                    MaterializeReaderActivityNode mra = new MaterializeReaderActivityNode(new ActivityId(odId,
                            activityId));
                    builder.addActivity(this, mra);
                    builder.addTargetEdge(i, mra, 0);
                    builder.addBlockingEdge(sma, mra);
                    numberOfActiveMaterializeReaders++;
                    activityId++;
                }
            }
        }
    }

    private final class SplitterMaterializerActivityNode extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        public SplitterMaterializerActivityNode(ActivityId id) {
            super(id);
        }

        @Override
        public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
                IRecordDescriptorProvider recordDescProvider, final int partition, int nPartitions) {
            return new AbstractUnaryInputOperatorNodePushable() {
                private MaterializerTaskState state;
                private final IFrameWriter[] writers = new IFrameWriter[numberOfNonMaterializedOutputs];
                private FrameTupleAccessor[] accessors;
                private ArrayTupleBuilder[] builders;
                private GrowableArray[] buildersData;
                private FrameTupleAppender[] appenders;
                private ByteBuffer[] writeBuffers;

                private FrameTupleAccessor accessor;
                private ArrayTupleBuilder builder;
                private GrowableArray builderData;
                private FrameTupleAppender appender;
                private ByteBuffer writeBuffer;

                @Override
                public void open() throws HyracksDataException {
                	if (conditionalSplitRequired) {
                    	accessors = new FrameTupleAccessor[numberOfNonMaterializedOutputs];
                        builders = new ArrayTupleBuilder[numberOfNonMaterializedOutputs];
                        buildersData = new GrowableArray[numberOfNonMaterializedOutputs];
                        appenders = new FrameTupleAppender[numberOfNonMaterializedOutputs];
                        writeBuffers = new ByteBuffer[numberOfNonMaterializedOutputs];

                    	for (int i = 0; i < numberOfNonMaterializedOutputs; i++) {
                            accessors[i] = new FrameTupleAccessor(ctx.getFrameSize(), inputRecordDesc);
                            writeBuffers[i] = ctx.allocateFrame();
                            builders[i] = new ArrayTupleBuilder(outputRecordDesc.getFieldCount());
                            buildersData[i] = builders[i].getFieldData();
                            appenders[i] = new FrameTupleAppender(ctx.getFrameSize());
                            appenders[i].reset(writeBuffers[i], true);
                        }

                        accessor = new FrameTupleAccessor(ctx.getFrameSize(), inputRecordDesc);
                        writeBuffer = ctx.allocateFrame();
                        builder = new ArrayTupleBuilder(outputRecordDesc.getFieldCount());
                        builderData = builder.getFieldData();
                        appender = new FrameTupleAppender(ctx.getFrameSize());
                        appender.reset(writeBuffer, true);

                	}

                    if (requiresMaterialization) {
                        state = new MaterializerTaskState(ctx.getJobletContext().getJobId(), new TaskId(
                                getActivityId(), partition));
                        state.open(ctx);
                    }
                    for (int i = 0; i < numberOfNonMaterializedOutputs; i++) {
                        writers[i].open();
                    }
                }

                @Override
                public void nextFrame(ByteBuffer bufferAccessor) throws HyracksDataException {
                    if (requiresMaterialization) {
                        state.appendFrame(bufferAccessor);
                    }

                    if (!conditionalSplitRequired) {
                        for (int i = 0; i < numberOfNonMaterializedOutputs; i++) {
                            FrameUtils.flushFrame(bufferAccessor, writers[i]);
                        }
                    } else {
                        accessor.reset(bufferAccessor);
                        int tupleCount = accessor.getTupleCount();

                        for (int i = 0; i < tupleCount; i++) {
                            builder.reset();

                        }











                    }
                }

                @Override
                public void close() throws HyracksDataException {
                    if (requiresMaterialization) {
                        state.close();
                        ctx.setStateObject(state);
                    }
                    for (int i = 0; i < numberOfNonMaterializedOutputs; i++) {
                        writers[i].close();
                    }
                }

                @Override
                public void fail() throws HyracksDataException {
                    for (int i = 0; i < numberOfNonMaterializedOutputs; i++) {
                        writers[i].fail();
                    }
                }

                @Override
                public void setOutputFrameWriter(int index, IFrameWriter writer, RecordDescriptor recordDesc) {
                    writers[index] = writer;
                }
            };
        }
    }

    private final class MaterializeReaderActivityNode extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        public MaterializeReaderActivityNode(ActivityId id) {
            super(id);
        }

        @Override
        public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
                final IRecordDescriptorProvider recordDescProvider, final int partition, int nPartitions)
                throws HyracksDataException {
            return new AbstractUnaryOutputSourceOperatorNodePushable() {

                @Override
                public void initialize() throws HyracksDataException {
                    MaterializerTaskState state = (MaterializerTaskState) ctx.getStateObject(new TaskId(new ActivityId(
                            getOperatorId(), SPLITTER_MATERIALIZER_ACTIVITY_ID), partition));
                    state.writeOut(writer, new VSizeFrame(ctx));
                }

                @Override
                public void deinitialize() throws HyracksDataException {
                    numberOfActiveMaterializeReaders--;
                    MaterializerTaskState state = (MaterializerTaskState) ctx.getStateObject(new TaskId(new ActivityId(
                            getOperatorId(), SPLITTER_MATERIALIZER_ACTIVITY_ID), partition));
                    if (numberOfActiveMaterializeReaders == 0) {
                        state.deleteFile();
                    }
                }
            };
        }
    }
}
