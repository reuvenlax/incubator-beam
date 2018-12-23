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
package org.apache.beam.sdk.schemas.utils;

import static org.apache.beam.sdk.schemas.utils.TestJavaBeans.BEAN_WITH_BOXED_FIELDS_SCHEMA;
import static org.apache.beam.sdk.schemas.utils.TestJavaBeans.BEAN_WITH_BYTE_ARRAY_SCHEMA;
import static org.apache.beam.sdk.schemas.utils.TestJavaBeans.NESTED_ARRAY_BEAN_SCHEMA;
import static org.apache.beam.sdk.schemas.utils.TestJavaBeans.NESTED_BEAN_SCHEMA;
import static org.apache.beam.sdk.schemas.utils.TestJavaBeans.NESTED_COLLECTION_BEAN_SCHEMA;
import static org.apache.beam.sdk.schemas.utils.TestJavaBeans.NESTED_MAP_BEAN_SCHEMA;
import static org.apache.beam.sdk.schemas.utils.TestJavaBeans.PRIMITIVE_ARRAY_BEAN_SCHEMA;
import static org.apache.beam.sdk.schemas.utils.TestJavaBeans.PRIMITIVE_MAP_BEAN_SCHEMA;
import static org.apache.beam.sdk.schemas.utils.TestJavaBeans.SIMPLE_BEAN_SCHEMA;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.List;
import org.apache.beam.sdk.schemas.FieldValueGetter;
import org.apache.beam.sdk.schemas.JavaBeanSchema;
import org.apache.beam.sdk.schemas.JavaBeanSchema.GetterTypeSupplier;
import org.apache.beam.sdk.schemas.JavaBeanSchema.SetterTypeSupplier;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaUserTypeCreator;
import org.apache.beam.sdk.schemas.utils.TestJavaBeans.BeanWithBoxedFields;
import org.apache.beam.sdk.schemas.utils.TestJavaBeans.BeanWithByteArray;
import org.apache.beam.sdk.schemas.utils.TestJavaBeans.MismatchingNullableBean;
import org.apache.beam.sdk.schemas.utils.TestJavaBeans.NestedArrayBean;
import org.apache.beam.sdk.schemas.utils.TestJavaBeans.NestedBean;
import org.apache.beam.sdk.schemas.utils.TestJavaBeans.NestedCollectionBean;
import org.apache.beam.sdk.schemas.utils.TestJavaBeans.NestedMapBean;
import org.apache.beam.sdk.schemas.utils.TestJavaBeans.NullableBean;
import org.apache.beam.sdk.schemas.utils.TestJavaBeans.PrimitiveArrayBean;
import org.apache.beam.sdk.schemas.utils.TestJavaBeans.PrimitiveMapBean;
import org.apache.beam.sdk.schemas.utils.TestJavaBeans.SimpleBean;
import org.joda.time.DateTime;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/** Tests for the {@link JavaBeanUtils} class. */
public class JavaBeanUtilsTest {
  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testNullable() {
    Schema schema =
        JavaBeanUtils.schemaFromJavaBeanClass(NullableBean.class, GetterTypeSupplier.INSTANCE);
    assertTrue(schema.getField("str").getType().getNullable());
    assertFalse(schema.getField("anInt").getType().getNullable());
  }

  @Test
  public void testMismatchingNullable() {
    thrown.expect(RuntimeException.class);
    Schema schema =
        JavaBeanUtils.schemaFromJavaBeanClass(
            MismatchingNullableBean.class, GetterTypeSupplier.INSTANCE);
  }

  @Test
  public void testSimpleBean() {
    Schema schema =
        JavaBeanUtils.schemaFromJavaBeanClass(SimpleBean.class, GetterTypeSupplier.INSTANCE);
    SchemaTestUtils.assertSchemaEquivalent(SIMPLE_BEAN_SCHEMA, schema);
  }

  @Test
  public void testNestedBean() {
    Schema schema =
        JavaBeanUtils.schemaFromJavaBeanClass(NestedBean.class, GetterTypeSupplier.INSTANCE);
    SchemaTestUtils.assertSchemaEquivalent(NESTED_BEAN_SCHEMA, schema);
  }

  @Test
  public void testPrimitiveArray() {
    Schema schema =
        JavaBeanUtils.schemaFromJavaBeanClass(
            PrimitiveArrayBean.class, GetterTypeSupplier.INSTANCE);
    SchemaTestUtils.assertSchemaEquivalent(PRIMITIVE_ARRAY_BEAN_SCHEMA, schema);
  }

  @Test
  public void testNestedArray() {
    Schema schema =
        JavaBeanUtils.schemaFromJavaBeanClass(NestedArrayBean.class, GetterTypeSupplier.INSTANCE);
    SchemaTestUtils.assertSchemaEquivalent(NESTED_ARRAY_BEAN_SCHEMA, schema);
  }

  @Test
  public void testNestedCollection() {
    Schema schema =
        JavaBeanUtils.schemaFromJavaBeanClass(
            NestedCollectionBean.class, GetterTypeSupplier.INSTANCE);
    SchemaTestUtils.assertSchemaEquivalent(NESTED_COLLECTION_BEAN_SCHEMA, schema);
  }

  @Test
  public void testPrimitiveMap() {
    Schema schema =
        JavaBeanUtils.schemaFromJavaBeanClass(PrimitiveMapBean.class, GetterTypeSupplier.INSTANCE);
    SchemaTestUtils.assertSchemaEquivalent(PRIMITIVE_MAP_BEAN_SCHEMA, schema);
  }

  @Test
  public void testNestedMap() {
    Schema schema =
        JavaBeanUtils.schemaFromJavaBeanClass(NestedMapBean.class, GetterTypeSupplier.INSTANCE);
    SchemaTestUtils.assertSchemaEquivalent(NESTED_MAP_BEAN_SCHEMA, schema);
  }

  @Test
  public void testGeneratedSimpleGetters() {
    SimpleBean simpleBean = new SimpleBean();
    simpleBean.setStr("field1");
    simpleBean.setaByte((byte) 41);
    simpleBean.setaShort((short) 42);
    simpleBean.setAnInt(43);
    simpleBean.setaLong(44);
    simpleBean.setaBoolean(true);
    simpleBean.setDateTime(DateTime.parse("1979-03-14"));
    simpleBean.setInstant(DateTime.parse("1979-03-15").toInstant());
    simpleBean.setBytes("bytes1".getBytes(Charset.defaultCharset()));
    simpleBean.setByteBuffer(ByteBuffer.wrap("bytes2".getBytes(Charset.defaultCharset())));
    simpleBean.setBigDecimal(new BigDecimal(42));
    simpleBean.setStringBuilder(new StringBuilder("stringBuilder"));

    List<FieldValueGetter> getters =
        JavaBeanUtils.getGetters(
            SimpleBean.class, SIMPLE_BEAN_SCHEMA, new JavaBeanSchema.GetterTypeSupplier());
    assertEquals(12, getters.size());
    assertEquals("str", getters.get(0).name());

    assertEquals("field1", getters.get(0).get(simpleBean));
    assertEquals((byte) 41, getters.get(1).get(simpleBean));
    assertEquals((short) 42, getters.get(2).get(simpleBean));
    assertEquals((int) 43, getters.get(3).get(simpleBean));
    assertEquals((long) 44, getters.get(4).get(simpleBean));
    assertEquals(true, getters.get(5).get(simpleBean));
    assertEquals(DateTime.parse("1979-03-14").toInstant(), getters.get(6).get(simpleBean));
    assertEquals(DateTime.parse("1979-03-15").toInstant(), getters.get(7).get(simpleBean));
    assertArrayEquals(
        "Unexpected bytes",
        "bytes1".getBytes(Charset.defaultCharset()),
        (byte[]) getters.get(8).get(simpleBean));
    assertArrayEquals(
        "Unexpected bytes",
        "bytes2".getBytes(Charset.defaultCharset()),
        (byte[]) getters.get(9).get(simpleBean));
    assertEquals(new BigDecimal(42), getters.get(10).get(simpleBean));
    assertEquals("stringBuilder", getters.get(11).get(simpleBean).toString());
  }

  @Test
  public void testGeneratedSimpleSetterCreator() {
    SchemaUserTypeCreator creator =
        JavaBeanUtils.getSetFieldCreator(
            SimpleBean.class, SIMPLE_BEAN_SCHEMA, new SetterTypeSupplier());
    SimpleBean simpleBean =
        (SimpleBean)
            creator.create(
                new Object[] {
                  "field1",
                  (byte) 41,
                  (short) 42,
                  (int) 43,
                  (long) 44,
                  true,
                  DateTime.parse("1979-03-14").toInstant(),
                  DateTime.parse("1979-03-15").toInstant(),
                  "bytes1".getBytes(Charset.defaultCharset()),
                  "bytes2".getBytes(Charset.defaultCharset()),
                  new BigDecimal(42),
                  "stringBuilder"
                });

    assertEquals("field1", simpleBean.getStr());
    assertEquals((byte) 41, simpleBean.getaByte());
    assertEquals((short) 42, simpleBean.getaShort());
    assertEquals((int) 43, simpleBean.getAnInt());
    assertEquals((long) 44, simpleBean.getaLong());
    assertEquals(true, simpleBean.isaBoolean());
    assertEquals(DateTime.parse("1979-03-14"), simpleBean.getDateTime());
    assertEquals(DateTime.parse("1979-03-15").toInstant(), simpleBean.getInstant());
    assertArrayEquals(
        "Unexpected bytes", "bytes1".getBytes(Charset.defaultCharset()), simpleBean.getBytes());
    assertEquals(
        ByteBuffer.wrap("bytes2".getBytes(Charset.defaultCharset())), simpleBean.getByteBuffer());
    assertEquals(new BigDecimal(42), simpleBean.getBigDecimal());
    assertEquals("stringBuilder", simpleBean.getStringBuilder().toString());
  }

  @Test
  public void testGeneratedSimpleBoxedGetters() {
    BeanWithBoxedFields bean = new BeanWithBoxedFields();
    bean.setaByte((byte) 41);
    bean.setaShort((short) 42);
    bean.setAnInt(43);
    bean.setaLong(44L);
    bean.setaBoolean(true);

    List<FieldValueGetter> getters =
        JavaBeanUtils.getGetters(
            BeanWithBoxedFields.class,
            BEAN_WITH_BOXED_FIELDS_SCHEMA,
            new JavaBeanSchema.GetterTypeSupplier());
    assertEquals((byte) 41, getters.get(0).get(bean));
    assertEquals((short) 42, getters.get(1).get(bean));
    assertEquals((int) 43, getters.get(2).get(bean));
    assertEquals((long) 44, getters.get(3).get(bean));
    assertEquals(true, getters.get(4).get(bean));
  }

  @Test
  public void testGeneratedSimpleBoxedSetterCreator() {
    SchemaUserTypeCreator creator =
        JavaBeanUtils.getSetFieldCreator(
            BeanWithBoxedFields.class, BEAN_WITH_BOXED_FIELDS_SCHEMA, new SetterTypeSupplier());
    BeanWithBoxedFields bean =
        (BeanWithBoxedFields)
            creator.create(new Object[] {(byte) 41, (short) 42, (int) 43, (long) 44, true});

    assertEquals((byte) 41, bean.getaByte().byteValue());
    assertEquals((short) 42, bean.getaShort().shortValue());
    assertEquals((int) 43, bean.getAnInt().intValue());
    assertEquals((long) 44, bean.getaLong().longValue());
    assertEquals(true, bean.getaBoolean().booleanValue());
  }

  @Test
  public void testGeneratedByteBufferSetterCreator() {
    SchemaUserTypeCreator creator =
        JavaBeanUtils.getSetFieldCreator(
            BeanWithByteArray.class, BEAN_WITH_BYTE_ARRAY_SCHEMA, new SetterTypeSupplier());
    BeanWithByteArray bean =
        (BeanWithByteArray)
            creator.create(
                new Object[] {
                  "field1".getBytes(Charset.defaultCharset()),
                  "field2".getBytes(Charset.defaultCharset())
                });

    assertArrayEquals("not equal", "field1".getBytes(Charset.defaultCharset()), bean.getBytes1());
    assertEquals(ByteBuffer.wrap("field2".getBytes(Charset.defaultCharset())), bean.getBytes2());
  }
}
