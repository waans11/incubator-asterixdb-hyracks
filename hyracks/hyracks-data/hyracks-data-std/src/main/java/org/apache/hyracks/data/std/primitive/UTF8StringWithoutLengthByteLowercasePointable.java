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
package org.apache.hyracks.data.std.primitive;

import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.data.std.api.AbstractPointable;
import org.apache.hyracks.data.std.api.IComparableForStringWithoutLengthByte;
import org.apache.hyracks.data.std.api.IHashableForStringWithoutLengthByte;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.api.IPointableFactory;
import org.apache.hyracks.util.string.UTF8StringUtil;

/**
 * This string pointable is for the UTF8 string that doesn't have length bytes in the beginning.
 * Instead, the length of this string is provided as a parameter.
 */
public final class UTF8StringWithoutLengthByteLowercasePointable extends AbstractPointable implements
        IHashableForStringWithoutLengthByte, IComparableForStringWithoutLengthByte {
    public static final ITypeTraits TYPE_TRAITS = new ITypeTraits() {
        private static final long serialVersionUID = 1L;

        @Override
        public boolean isFixedLength() {
            return false;
        }

        @Override
        public int getFixedLength() {
            return 0;
        }
    };

    public static final IPointableFactory FACTORY = new IPointableFactory() {
        private static final long serialVersionUID = 1L;

        @Override
        public IPointable createPointable() {
            return new UTF8StringWithoutLengthByteLowercasePointable();
        }

        @Override
        public ITypeTraits getTypeTraits() {
            return TYPE_TRAITS;
        }
    };

    public static int getUTFLen(byte[] b, int s) {
        return ((b[s] & 0xff) << 8) + ((b[s + 1] & 0xff) << 0);
    }

    // If the length is given, this method assumes that it only receives the string literal
    // without length (2 byte) in the beginning.
    public int compareToWithoutLengthByte(byte[] bytes, int start, int length) {
        return UTF8StringUtil.lowerCaseCompareTo(this.bytes, this.start, this.length, bytes, start, length);
    }

    // If the length is given, this method assumes that it only receives the string literal
    // without length (2 byte) in the beginning.
    @Override
    public int hash(int length) {
        return UTF8StringUtil.lowerCaseHash(bytes, start, length);
    }

}
