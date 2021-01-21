/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.db.marshal;

import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.function.Supplier;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.utils.UUIDGen;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.quicktheories.core.Gen;
import org.quicktheories.impl.Constraint;

import static org.quicktheories.QuickTheory.qt;
import static org.quicktheories.generators.SourceDSL.bigDecimals;
import static org.quicktheories.generators.SourceDSL.bigIntegers;
import static org.quicktheories.generators.SourceDSL.floats;
import static org.quicktheories.generators.SourceDSL.integers;
import static org.quicktheories.generators.SourceDSL.strings;

public class ByteComparableConversionAndComparisonTest
{
    private final static Logger logger = LoggerFactory.getLogger(ByteComparableConversionAndComparisonTest.class);

    private static <T> void testRoundTrip(AbstractType<T> typeInstance, Supplier<Gen<T>> generatorProvider)
    {
        qt().forAll(generatorProvider.get()).check(i -> {
            ByteBuffer bb = typeInstance.decompose(i);
            ByteSource bs = typeInstance.asComparableBytes(bb, ByteComparable.Version.OSS41);
            ByteBuffer bbRecreated = typeInstance.fromComparableBytes(ByteSource.peekable(bs), ByteComparable.Version.OSS41);
            T iRecreated = typeInstance.compose(bbRecreated);

            boolean resultEquals = i.equals(iRecreated);
            boolean resultCompare = typeInstance.compare(bb, bbRecreated) == 0;

            if (!resultEquals || !resultCompare)
            {
                logger.error("Round trip failed: {} != {}: equals: {}, compare: {}", i, iRecreated, resultEquals, resultCompare);
            }

            return resultEquals && resultCompare;
        });
    }

    private static <T> void testCompare(AbstractType<T> typeInstance, Supplier<Gen<T>> generatorProvider)
    {
        qt().forAll(generatorProvider.get(), generatorProvider.get()).check((i1, i2) -> {
            ByteBuffer bb1 = typeInstance.decompose(i1);
            ByteBuffer bb2 = typeInstance.decompose(i2);

            ByteSource bs1 = typeInstance.asComparableBytes(bb1, ByteComparable.Version.OSS41);
            ByteSource bs2 = typeInstance.asComparableBytes(bb2, ByteComparable.Version.OSS41);

            int comparison;
            while (true)
            {
                int b1 = bs1.next();
                int b2 = bs2.next();
                comparison = Integer.compare(b1, b2);
                if (comparison != 0 || b1 == ByteSource.END_OF_STREAM)
                    break;
            }

            int expectedComparison = Integer.signum(typeInstance.compare(bb1, bb2));
            return comparison == expectedComparison;
        });
    }

    private static <T> void testType(AbstractType<T> typeInstance, Supplier<Gen<T>> generatorProvider)
    {
        testRoundTrip(typeInstance, generatorProvider);
        testCompare(typeInstance, generatorProvider);
    }


    @Test
    public void testFloatType()
    {
        testType(FloatType.instance, () -> floats().any());
    }

    @Test
    public void testIntegerType()
    {
        testType(IntegerType.instance, () -> bigIntegers().ofBytes(3));
    }

    @Test
    public void testDecimalType()
    {

        testType(DecimalType.instance, () -> bigDecimals().ofBytes(3).withScale(0));
        testType(DecimalType.instance, () -> bigDecimals().ofBytes(3).withScale(-10));
        testType(DecimalType.instance, () -> bigDecimals().ofBytes(3).withScale(10));
    }

    @Test
    public void testInt32Type()
    {
        testType(Int32Type.instance, () -> integers().all());
    }

    @Test
    public void testUTF8Type()
    {
        testType(UTF8Type.instance, () -> strings().basicMultilingualPlaneAlphabet().ofLengthBetween(0, 3));
    }

    @Test
    public void testUUIDType()
    {
        testType(UUIDType.instance, () -> randomnessSource -> {
            long l1 = randomnessSource.next(Constraint.none());
            long l2 = randomnessSource.next(Constraint.none());
            return new UUID(l1, l2);
        });
    }

    @Test
    public void testTimeUUID()
    {
        long now = System.currentTimeMillis();
        long range = 1000L * 3600L * 24L * 365L;
        testType(TimeUUIDType.instance, () -> randomnessSource -> {
            long ts = randomnessSource.next(Constraint.between(now - range, now + range));
            long nanos = randomnessSource.next(Constraint.between(0, 999999));
            long clk = randomnessSource.next(Constraint.none());
            return UUIDGen.getTimeUUID(ts, nanos, clk);
        });
    }
}
