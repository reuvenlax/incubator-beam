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
package org.apache.beam.sdk.schemas;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import java.util.List;
import java.util.stream.Collectors;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.TokenStream;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor.FieldDescriptor;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor.FieldDescriptor.ListQualifier;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor.FieldDescriptor.MapQualifier;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor.FieldDescriptor.Qualifier;
import org.apache.beam.sdk.schemas.FieldSpecifierNotationParser.ArrayQualifierListContext;
import org.apache.beam.sdk.schemas.FieldSpecifierNotationParser.DotExpressionContext;
import org.apache.beam.sdk.schemas.FieldSpecifierNotationParser.FieldSpecifierContext;
import org.apache.beam.sdk.schemas.FieldSpecifierNotationParser.MapQualifierListContext;
import org.apache.beam.sdk.schemas.FieldSpecifierNotationParser.QualifiedComponentContext;
import org.apache.beam.sdk.schemas.FieldSpecifierNotationParser.QualifyComponentContext;
import org.apache.beam.sdk.schemas.FieldSpecifierNotationParser.SimpleIdentifierContext;
import org.apache.beam.sdk.schemas.FieldSpecifierNotationParser.WildcardContext;

class FieldAccessDescriptorParser {
  static FieldAccessDescriptor parse(String expr) {
    CharStream charStream = CharStreams.fromString(expr);
    org.apache.beam.sdk.schemas.FieldSpecifierNotationLexer lexer =
        new org.apache.beam.sdk.schemas.FieldSpecifierNotationLexer(charStream);
    TokenStream tokens = new CommonTokenStream(lexer);
    org.apache.beam.sdk.schemas.FieldSpecifierNotationParser parser =
        new org.apache.beam.sdk.schemas.FieldSpecifierNotationParser(tokens);
    return new BuildFieldAccessDescriptor().visit(parser.dotExpression());
  }

  public static class BuildFieldAccessDescriptor
      extends org.apache.beam.sdk.schemas.FieldSpecifierNotationBaseVisitor<FieldAccessDescriptor> {

    @Override
    public FieldAccessDescriptor visitFieldSpecifier(FieldSpecifierContext ctx) {
      return ctx.dotExpression().accept(this);
    }

    @Override
    public FieldAccessDescriptor visitDotExpression(DotExpressionContext ctx) {
      List<FieldAccessDescriptor> components =
          ctx.dotExpressionComponent()
              .stream()
              .map(dotE -> dotE.accept(this))
              .collect(Collectors.toList());

      // Walk backwards through the list to build up the nested FieldAccessDescriptor.
      Preconditions.checkArgument(!components.isEmpty());
      FieldAccessDescriptor fieldAccessDescriptor = components.get(components.size() - 1);
      for (int i = components.size() - 2; i >= 0; --i) {
        FieldAccessDescriptor component = components.get(i);
        if (component.getAllFields()) {
          // TODO: what semantics of x.*.y? maybe we need to expand.
          fieldAccessDescriptor = FieldAccessDescriptor.withAllFields();
          continue;
        }
        FieldDescriptor fieldAccessed =
            component
                .getFieldsAccessed()
                .stream()
                .findFirst()
                .orElseThrow(IllegalArgumentException::new);

        fieldAccessDescriptor =
            FieldAccessDescriptor.withFields()
                .withNestedField(fieldAccessed, fieldAccessDescriptor);
      }
      return fieldAccessDescriptor;
    }

    @Override
    public FieldAccessDescriptor visitQualifyComponent(QualifyComponentContext ctx) {
      return ctx.qualifiedComponent().accept(this);
    }

    @Override
    public FieldAccessDescriptor visitSimpleIdentifier(SimpleIdentifierContext ctx) {
      FieldDescriptor field =
          FieldDescriptor.builder().setFieldName(ctx.IDENTIFIER().getText()).build();
      return FieldAccessDescriptor.withFields(field);
    }

    @Override
    public FieldAccessDescriptor visitWildcard(WildcardContext ctx) {
      return FieldAccessDescriptor.withAllFields();
    }

    @Override
    public FieldAccessDescriptor visitQualifiedComponent(QualifiedComponentContext ctx) {
      QualifierVisitor qualifierVisitor = new QualifierVisitor();
      ctx.qualifierList().accept(qualifierVisitor);
      FieldDescriptor field =
          FieldDescriptor.builder()
              .setFieldName(ctx.IDENTIFIER().getText())
              .setQualifiers(qualifierVisitor.getQualifiers())
              .build();
      return FieldAccessDescriptor.withFields(field);
    }
  }

  public static class QualifierVisitor
      extends org.apache.beam.sdk.schemas.FieldSpecifierNotationBaseVisitor<FieldAccessDescriptor> {
    private final List<Qualifier> qualifiers = Lists.newArrayList();

    @Override
    public FieldAccessDescriptor visitArrayQualifierList(ArrayQualifierListContext ctx) {
      qualifiers.add(Qualifier.of(new ListQualifier()));
      ctx.qualifierList().forEach(subList -> subList.accept(this));
      return null;
    }

    @Override
    public FieldAccessDescriptor visitMapQualifierList(MapQualifierListContext ctx) {
      qualifiers.add(Qualifier.of(new MapQualifier()));
      ctx.qualifierList().forEach(subList -> subList.accept(this));
      return null;
    }

    public List<Qualifier> getQualifiers() {
      return qualifiers;
    }
  }
}
