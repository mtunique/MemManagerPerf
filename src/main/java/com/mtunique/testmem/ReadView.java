package com.mtunique.testmem;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.SeekableDataInputView;
import org.apache.flink.runtime.memory.AbstractPagedInputView;
import org.apache.flink.util.MathUtils;

import java.io.EOFException;
import java.util.List;

/**
 * Created by taomeng on 2016/12/29.
 */
public class ReadView extends AbstractPagedInputView implements SeekableDataInputView {

    private final List<MemorySegment> segments;

    private final int segmentSizeBits;

    private final int segmentSizeMask;

    private int currentSegmentIndex;

    private int segmentNumberOffset;


    public ReadView(List<MemorySegment> segments, int segmentSize) {
        super(segments.get(0), segmentSize, 0);

        if ((segmentSize & (segmentSize - 1)) != 0) {
            throw new IllegalArgumentException("Segment size must be a power of 2!");
        }

        this.segments = segments;
        this.segmentSizeBits = MathUtils.log2strict(segments.get(0).size());
        this.segmentSizeMask = segmentSize - 1;
        this.segmentNumberOffset = 0;
    }

    @Override
    protected MemorySegment nextSegment(MemorySegment current) throws EOFException {
        if (++this.currentSegmentIndex < this.segments.size()) {
            return this.segments.get(this.currentSegmentIndex);
        } else {
            throw new EOFException();
        }
    }

    @Override
    protected int getLimitForSegment(MemorySegment segment) {
        return this.segmentSizeMask + 1;
    }

    public void setReadPosition(long position) {
        final int bufferNum = ((int) (position >>> this.segmentSizeBits)) - this.segmentNumberOffset;
        final int offset = (int) (position & this.segmentSizeMask);

        this.currentSegmentIndex = bufferNum;
        seekInput(this.segments.get(bufferNum), offset, this.segmentSizeMask + 1);
    }

    @SuppressWarnings("unused")
    public void setSegmentNumberOffset(int offset) {
        this.segmentNumberOffset = offset;
    }
}
