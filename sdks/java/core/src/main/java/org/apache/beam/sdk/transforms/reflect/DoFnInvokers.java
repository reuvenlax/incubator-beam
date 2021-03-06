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
package org.apache.beam.sdk.transforms.reflect;

import static com.google.common.base.Preconditions.checkArgument;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.NamingStrategy;
import net.bytebuddy.description.field.FieldDescription;
import net.bytebuddy.description.method.MethodDescription;
import net.bytebuddy.description.modifier.FieldManifestation;
import net.bytebuddy.description.modifier.Visibility;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.description.type.TypeList;
import net.bytebuddy.dynamic.DynamicType;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.dynamic.scaffold.InstrumentedType;
import net.bytebuddy.dynamic.scaffold.subclass.ConstructorStrategy;
import net.bytebuddy.implementation.ExceptionMethod;
import net.bytebuddy.implementation.FixedValue;
import net.bytebuddy.implementation.Implementation;
import net.bytebuddy.implementation.Implementation.Context;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.TargetMethodAnnotationDrivenBinder;
import net.bytebuddy.implementation.bytecode.ByteCodeAppender;
import net.bytebuddy.implementation.bytecode.StackManipulation;
import net.bytebuddy.implementation.bytecode.Throw;
import net.bytebuddy.implementation.bytecode.assign.Assigner;
import net.bytebuddy.implementation.bytecode.assign.TypeCasting;
import net.bytebuddy.implementation.bytecode.member.FieldAccess;
import net.bytebuddy.implementation.bytecode.member.MethodInvocation;
import net.bytebuddy.implementation.bytecode.member.MethodReturn;
import net.bytebuddy.implementation.bytecode.member.MethodVariableAccess;
import net.bytebuddy.jar.asm.Label;
import net.bytebuddy.jar.asm.MethodVisitor;
import net.bytebuddy.jar.asm.Opcodes;
import net.bytebuddy.jar.asm.Type;
import net.bytebuddy.matcher.ElementMatchers;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.ExtraContextFactory;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.transforms.DoFnAdapters;
import org.apache.beam.sdk.transforms.OldDoFn;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.util.UserCodeException;
import org.apache.beam.sdk.values.TypeDescriptor;

/** Dynamically generates {@link DoFnInvoker} instances for invoking a {@link DoFn}. */
public class DoFnInvokers {
  public static final DoFnInvokers INSTANCE = new DoFnInvokers();

  private static final String FN_DELEGATE_FIELD_NAME = "delegate";

  /**
   * A cache of constructors of generated {@link DoFnInvoker} classes, keyed by {@link DoFn} class.
   * Needed because generating an invoker class is expensive, and to avoid generating an excessive
   * number of classes consuming PermGen memory.
   */
  private final Map<Class<?>, Constructor<?>> byteBuddyInvokerConstructorCache =
      new LinkedHashMap<>();

  private DoFnInvokers() {}

  /**
   * Creates a {@link DoFnInvoker} for the given {@link Object}, which should be either a {@link
   * DoFn} or an {@link OldDoFn}. The expected use would be to deserialize a user's function as an
   * {@link Object} and then pass it to this method, so there is no need to statically specify what
   * sort of object it is.
   *
   * @deprecated this is to be used only as a migration path for decoupling upgrades
   */
  @Deprecated
  public DoFnInvoker<?, ?> invokerFor(Object deserializedFn) {
    if (deserializedFn instanceof DoFn) {
      return newByteBuddyInvoker((DoFn<?, ?>) deserializedFn);
    } else if (deserializedFn instanceof OldDoFn) {
      return new OldDoFnInvoker<>((OldDoFn<?, ?>) deserializedFn);
    } else {
      throw new IllegalArgumentException(
          String.format(
              "Cannot create a %s for %s; it should be either a %s or an %s.",
              DoFnInvoker.class.getSimpleName(),
              deserializedFn.toString(),
              DoFn.class.getSimpleName(),
              OldDoFn.class.getSimpleName()));
    }
  }

  static class OldDoFnInvoker<InputT, OutputT> implements DoFnInvoker<InputT, OutputT> {

    private final OldDoFn<InputT, OutputT> fn;

    public OldDoFnInvoker(OldDoFn<InputT, OutputT> fn) {
      this.fn = fn;
    }

    @Override
    public DoFn.ProcessContinuation invokeProcessElement(
        DoFn<InputT, OutputT>.ProcessContext c, ExtraContextFactory<InputT, OutputT> extra) {
      OldDoFn<InputT, OutputT>.ProcessContext oldCtx =
          DoFnAdapters.adaptProcessContext(fn, c, extra);
      try {
        fn.processElement(oldCtx);
        return DoFn.ProcessContinuation.stop();
      } catch (Throwable exc) {
        throw UserCodeException.wrap(exc);
      }
    }

    @Override
    public void invokeStartBundle(DoFn.Context c) {
      OldDoFn<InputT, OutputT>.Context oldCtx = DoFnAdapters.adaptContext(fn, c);
      try {
        fn.startBundle(oldCtx);
      } catch (Throwable exc) {
        throw UserCodeException.wrap(exc);
      }
    }

    @Override
    public void invokeFinishBundle(DoFn.Context c) {
      OldDoFn<InputT, OutputT>.Context oldCtx = DoFnAdapters.adaptContext(fn, c);
      try {
        fn.finishBundle(oldCtx);
      } catch (Throwable exc) {
        throw UserCodeException.wrap(exc);
      }
    }

    @Override
    public void invokeSetup() {
      try {
        fn.setup();
      } catch (Throwable exc) {
        throw UserCodeException.wrap(exc);
      }
    }

    @Override
    public void invokeTeardown() {
      try {
        fn.teardown();
      } catch (Throwable exc) {
        throw UserCodeException.wrap(exc);
      }
    }

    @Override
    public <RestrictionT> RestrictionT invokeGetInitialRestriction(InputT element) {
      throw new UnsupportedOperationException("OldDoFn is not splittable");
    }

    @Override
    public <RestrictionT> Coder<RestrictionT> invokeGetRestrictionCoder(
        CoderRegistry coderRegistry) {
      throw new UnsupportedOperationException("OldDoFn is not splittable");
    }

    @Override
    public <RestrictionT> void invokeSplitRestriction(
        InputT element, RestrictionT restriction, DoFn.OutputReceiver<RestrictionT> receiver) {
      throw new UnsupportedOperationException("OldDoFn is not splittable");
    }

    @Override
    public <RestrictionT, TrackerT extends RestrictionTracker<RestrictionT>>
        TrackerT invokeNewTracker(RestrictionT restriction) {
      throw new UnsupportedOperationException("OldDoFn is not splittable");
    }
  }

  /** @return the {@link DoFnInvoker} for the given {@link DoFn}. */
  @SuppressWarnings({"unchecked", "rawtypes"})
  public <InputT, OutputT> DoFnInvoker<InputT, OutputT> newByteBuddyInvoker(
      DoFn<InputT, OutputT> fn) {
    return newByteBuddyInvoker(
        DoFnSignatures.INSTANCE.getSignature((Class) fn.getClass()), fn);
  }

  /** @return the {@link DoFnInvoker} for the given {@link DoFn}. */
  public <InputT, OutputT> DoFnInvoker<InputT, OutputT> newByteBuddyInvoker(
      DoFnSignature signature, DoFn<InputT, OutputT> fn) {
    checkArgument(
        signature.fnClass().equals(fn.getClass()),
        "Signature is for class %s, but fn is of class %s",
        signature.fnClass(),
        fn.getClass());
    try {
      @SuppressWarnings("unchecked")
      DoFnInvoker<InputT, OutputT> invoker =
          (DoFnInvoker<InputT, OutputT>)
              getByteBuddyInvokerConstructor(signature).newInstance(fn);
      return invoker;
    } catch (InstantiationException
        | IllegalAccessException
        | IllegalArgumentException
        | InvocationTargetException
        | SecurityException e) {
      throw new RuntimeException("Unable to bind invoker for " + fn.getClass(), e);
    }
  }

  /**
   * Returns a generated constructor for a {@link DoFnInvoker} for the given {@link DoFn} class.
   *
   * <p>These are cached such that at most one {@link DoFnInvoker} class exists for a given
   * {@link DoFn} class.
   */
  private synchronized Constructor<?> getByteBuddyInvokerConstructor(
      DoFnSignature signature) {
    Class<? extends DoFn<?, ?>> fnClass = signature.fnClass();
    Constructor<?> constructor = byteBuddyInvokerConstructorCache.get(fnClass);
    if (constructor == null) {
      Class<? extends DoFnInvoker<?, ?>> invokerClass = generateInvokerClass(signature);
      try {
        constructor = invokerClass.getConstructor(fnClass);
      } catch (IllegalArgumentException | NoSuchMethodException | SecurityException e) {
        throw new RuntimeException(e);
      }
      byteBuddyInvokerConstructorCache.put(fnClass, constructor);
    }
    return constructor;
  }

  /** Default implementation of {@link DoFn.SplitRestriction}, for delegation by bytebuddy. */
  public static class DefaultSplitRestriction {
    /** Doesn't split the restriction. */
    @SuppressWarnings("unused")
    public static <InputT, RestrictionT> void invokeSplitRestriction(
        InputT element, RestrictionT restriction, DoFn.OutputReceiver<RestrictionT> receiver) {
      receiver.output(restriction);
    }
  }

  /** Default implementation of {@link DoFn.GetRestrictionCoder}, for delegation by bytebuddy. */
  public static class DefaultRestrictionCoder {
    private final TypeDescriptor<?> restrictionType;

    DefaultRestrictionCoder(TypeDescriptor<?> restrictionType) {
      this.restrictionType = restrictionType;
    }

    /** Doesn't split the restriction. */
    @SuppressWarnings({"unused", "unchecked"})
    public <RestrictionT> Coder<RestrictionT> invokeGetRestrictionCoder(CoderRegistry registry)
        throws CannotProvideCoderException {
      return (Coder) registry.getCoder(restrictionType);
    }
  }

  /** Generates a {@link DoFnInvoker} class for the given {@link DoFnSignature}. */
  private static Class<? extends DoFnInvoker<?, ?>> generateInvokerClass(DoFnSignature signature) {
    Class<? extends DoFn<?, ?>> fnClass = signature.fnClass();

    final TypeDescription clazzDescription = new TypeDescription.ForLoadedType(fnClass);

    DynamicType.Builder<?> builder =
        new ByteBuddy()
            // Create subclasses inside the target class, to have access to
            // private and package-private bits
            .with(
                new NamingStrategy.SuffixingRandom("auxiliary") {
                  @Override
                  public String subclass(TypeDescription.Generic superClass) {
                    return super.name(clazzDescription);
                  }
                })
            // Create a subclass of DoFnInvoker
            .subclass(DoFnInvoker.class, ConstructorStrategy.Default.NO_CONSTRUCTORS)
            .defineField(
                FN_DELEGATE_FIELD_NAME, fnClass, Visibility.PRIVATE, FieldManifestation.FINAL)
            .defineConstructor(Visibility.PUBLIC)
            .withParameter(fnClass)
            .intercept(new InvokerConstructor())
            .method(ElementMatchers.named("invokeProcessElement"))
            .intercept(new ProcessElementDelegation(signature.processElement()))
            .method(ElementMatchers.named("invokeStartBundle"))
            .intercept(delegateOrNoop(signature.startBundle()))
            .method(ElementMatchers.named("invokeFinishBundle"))
            .intercept(delegateOrNoop(signature.finishBundle()))
            .method(ElementMatchers.named("invokeSetup"))
            .intercept(delegateOrNoop(signature.setup()))
            .method(ElementMatchers.named("invokeTeardown"))
            .intercept(delegateOrNoop(signature.teardown()))
            .method(ElementMatchers.named("invokeGetInitialRestriction"))
            .intercept(delegateWithDowncastOrThrow(signature.getInitialRestriction()))
            .method(ElementMatchers.named("invokeSplitRestriction"))
            .intercept(splitRestrictionDelegation(signature))
            .method(ElementMatchers.named("invokeGetRestrictionCoder"))
            .intercept(getRestrictionCoderDelegation(signature))
            .method(ElementMatchers.named("invokeNewTracker"))
            .intercept(delegateWithDowncastOrThrow(signature.newTracker()));

    DynamicType.Unloaded<?> unloaded = builder.make();

    @SuppressWarnings("unchecked")
    Class<? extends DoFnInvoker<?, ?>> res =
        (Class<? extends DoFnInvoker<?, ?>>)
            unloaded
                .load(DoFnInvokers.class.getClassLoader(), ClassLoadingStrategy.Default.INJECTION)
                .getLoaded();
    return res;
  }

  private static Implementation getRestrictionCoderDelegation(DoFnSignature signature) {
    if (signature.processElement().isSplittable()) {
      if (signature.getRestrictionCoder() == null) {
        return MethodDelegation.to(
            new DefaultRestrictionCoder(signature.getInitialRestriction().restrictionT()));
      } else {
        return new DowncastingParametersMethodDelegation(
            signature.getRestrictionCoder().targetMethod());
      }
    } else {
      return ExceptionMethod.throwing(UnsupportedOperationException.class);
    }
  }

  private static Implementation splitRestrictionDelegation(DoFnSignature signature) {
    if (signature.splitRestriction() == null) {
      return MethodDelegation.to(DefaultSplitRestriction.class);
    } else {
      return new DowncastingParametersMethodDelegation(signature.splitRestriction().targetMethod());
    }
  }

  /** Delegates to the given method if available, or does nothing. */
  private static Implementation delegateOrNoop(DoFnSignature.DoFnMethod method) {
    return (method == null)
        ? FixedValue.originType()
        : new DoFnMethodDelegation(method.targetMethod());
  }

  /** Delegates to the given method if available, or throws UnsupportedOperationException. */
  private static Implementation delegateWithDowncastOrThrow(DoFnSignature.DoFnMethod method) {
    return (method == null)
        ? ExceptionMethod.throwing(UnsupportedOperationException.class)
        : new DowncastingParametersMethodDelegation(method.targetMethod());
  }

  /**
   * Implements a method of {@link DoFnInvoker} (the "instrumented method") by delegating to a
   * "target method" of the wrapped {@link DoFn}.
   */
  private static class DoFnMethodDelegation implements Implementation {
    /** The {@link MethodDescription} of the wrapped {@link DoFn}'s method. */
    protected final MethodDescription targetMethod;
    /** Whether the target method returns non-void. */
    private final boolean targetHasReturn;

    private FieldDescription delegateField;

    public DoFnMethodDelegation(Method targetMethod) {
      this.targetMethod = new MethodDescription.ForLoadedMethod(targetMethod);
      targetHasReturn = !TypeDescription.VOID.equals(this.targetMethod.getReturnType().asErasure());
    }

    @Override
    public InstrumentedType prepare(InstrumentedType instrumentedType) {
      // Remember the field description of the instrumented type.
      delegateField =
          instrumentedType
              .getDeclaredFields()
              .filter(ElementMatchers.named(FN_DELEGATE_FIELD_NAME))
              .getOnly();
      // Delegating the method call doesn't require any changes to the instrumented type.
      return instrumentedType;
    }

    @Override
    public ByteCodeAppender appender(final Target implementationTarget) {
      return new ByteCodeAppender() {
        /**
         * @param instrumentedMethod The {@link DoFnInvoker} method for which we're generating code.
         */
        @Override
        public Size apply(
            MethodVisitor methodVisitor,
            Context implementationContext,
            MethodDescription instrumentedMethod) {
          // Figure out how many locals we'll need. This corresponds to "this", the parameters
          // of the instrumented method, and an argument to hold the return value if the target
          // method has a return value.
          int numLocals = 1 + instrumentedMethod.getParameters().size() + (targetHasReturn ? 1 : 0);

          Integer returnVarIndex = null;
          if (targetHasReturn) {
            // Local comes after formal parameters, so figure out where that is.
            returnVarIndex = 1; // "this"
            for (Type param : Type.getArgumentTypes(instrumentedMethod.getDescriptor())) {
              returnVarIndex += param.getSize();
            }
          }

          StackManipulation manipulation =
              new StackManipulation.Compound(
                  // Push "this" (DoFnInvoker on top of the stack)
                  MethodVariableAccess.REFERENCE.loadOffset(0),
                  // Access this.delegate (DoFn on top of the stack)
                  FieldAccess.forField(delegateField).getter(),
                  // Run the beforeDelegation manipulations.
                  // The arguments necessary to invoke the target are on top of the stack.
                  beforeDelegation(instrumentedMethod),
                  // Perform the method delegation.
                  // This will consume the arguments on top of the stack
                  // Either the stack is now empty (because the targetMethod returns void) or the
                  // stack contains the return value.
                  new UserCodeMethodInvocation(returnVarIndex, targetMethod, instrumentedMethod),
                  // Run the afterDelegation manipulations.
                  // Either the stack is now empty (because the instrumentedMethod returns void)
                  // or the stack contains the return value.
                  afterDelegation(instrumentedMethod));

          StackManipulation.Size size = manipulation.apply(methodVisitor, implementationContext);
          return new Size(size.getMaximalSize(), numLocals);
        }
      };
    }

    /**
     * Return the code to the prepare the operand stack for the method delegation.
     *
     * <p>Before this method is called, the stack delegate will be the only thing on the stack.
     *
     * <p>After this method is called, the stack contents should contain exactly the arguments
     * necessary to invoke the target method.
     */
    protected StackManipulation beforeDelegation(MethodDescription instrumentedMethod) {
      return MethodVariableAccess.allArgumentsOf(targetMethod);
    }

    /**
     * Return the code to execute after the method delegation.
     *
     * <p>Before this method is called, the stack will either be empty (if the target method returns
     * void) or contain the method return value.
     *
     * <p>After this method is called, the stack should either be empty (if the instrumented method
     * returns void) or contain the value for the instrumented method to return).
     */
    protected StackManipulation afterDelegation(MethodDescription instrumentedMethod) {
      return TargetMethodAnnotationDrivenBinder.TerminationHandler.Returning.INSTANCE.resolve(
          Assigner.DEFAULT, instrumentedMethod, targetMethod);
    }
  }

  /**
   * Passes parameters to the delegated method by downcasting each parameter of non-primitive type
   * to its expected type.
   */
  private static class DowncastingParametersMethodDelegation extends DoFnMethodDelegation {
    DowncastingParametersMethodDelegation(Method method) {
      super(method);
    }

    @Override
    protected StackManipulation beforeDelegation(MethodDescription instrumentedMethod) {
      List<StackManipulation> pushParameters = new ArrayList<>();
      TypeList.Generic paramTypes = targetMethod.getParameters().asTypeList();
      for (int i = 0; i < paramTypes.size(); i++) {
        TypeDescription.Generic paramT = paramTypes.get(i);
        pushParameters.add(MethodVariableAccess.of(paramT).loadOffset(i + 1));
        if (!paramT.isPrimitive()) {
          pushParameters.add(TypeCasting.to(paramT));
        }
      }
      return new StackManipulation.Compound(pushParameters);
    }
  }

  /**
   * Implements the invoker's {@link DoFnInvoker#invokeProcessElement} method by delegating to the
   * {@link DoFn.ProcessElement} method.
   */
  private static final class ProcessElementDelegation extends DoFnMethodDelegation {
    private static final Map<DoFnSignature.Parameter, MethodDescription>
        EXTRA_CONTEXT_FACTORY_METHODS;
    private static final MethodDescription PROCESS_CONTINUATION_STOP_METHOD;

    static {
      try {
        Map<DoFnSignature.Parameter, MethodDescription> methods = new HashMap<>();
        methods.put(
            DoFnSignature.Parameter.boundedWindow(),
            new MethodDescription.ForLoadedMethod(
                DoFn.ExtraContextFactory.class.getMethod("window")));
        methods.put(
            DoFnSignature.Parameter.inputProvider(),
            new MethodDescription.ForLoadedMethod(
                DoFn.ExtraContextFactory.class.getMethod("inputProvider")));
        methods.put(
            DoFnSignature.Parameter.outputReceiver(),
            new MethodDescription.ForLoadedMethod(
                DoFn.ExtraContextFactory.class.getMethod("outputReceiver")));
        methods.put(
            DoFnSignature.Parameter.restrictionTracker(),
            new MethodDescription.ForLoadedMethod(
                DoFn.ExtraContextFactory.class.getMethod("restrictionTracker")));
        EXTRA_CONTEXT_FACTORY_METHODS = Collections.unmodifiableMap(methods);
      } catch (Exception e) {
        throw new RuntimeException(
            "Failed to locate an ExtraContextFactory method that was expected to exist", e);
      }
      try {
        PROCESS_CONTINUATION_STOP_METHOD =
            new MethodDescription.ForLoadedMethod(DoFn.ProcessContinuation.class.getMethod("stop"));
      } catch (NoSuchMethodException e) {
        throw new RuntimeException("Failed to locate ProcessContinuation.stop()");
      }
    }

    private static MethodDescription getExtraContextFactoryMethod(DoFnSignature.Parameter param) {
      return EXTRA_CONTEXT_FACTORY_METHODS.get(param);
    }

    private final DoFnSignature.ProcessElementMethod signature;

    /** Implementation of {@link MethodDelegation} for the {@link ProcessElement} method. */
    private ProcessElementDelegation(DoFnSignature.ProcessElementMethod signature) {
      super(signature.targetMethod());
      this.signature = signature;
    }

    @Override
    protected StackManipulation beforeDelegation(MethodDescription instrumentedMethod) {
      // Parameters of the wrapper invoker method:
      //   DoFn.ProcessContext, ExtraContextFactory.
      // Parameters of the wrapped DoFn method:
      //   DoFn.ProcessContext, [BoundedWindow, InputProvider, OutputReceiver] in any order
      ArrayList<StackManipulation> parameters = new ArrayList<>();
      // Push the ProcessContext argument.
      parameters.add(MethodVariableAccess.REFERENCE.loadOffset(1));
      // Push the extra arguments in their actual order.
      StackManipulation pushExtraContextFactory = MethodVariableAccess.REFERENCE.loadOffset(2);
      for (DoFnSignature.Parameter param : signature.extraParameters()) {
        parameters.add(
            new StackManipulation.Compound(
                pushExtraContextFactory,
                MethodInvocation.invoke(getExtraContextFactoryMethod(param)),
                // ExtraContextFactory.restrictionTracker() returns a RestrictionTracker,
                // but the @ProcessElement method expects a concrete subtype of it.
                // Insert a downcast.
                DoFnSignature.Parameter.restrictionTracker().equals(param)
                    ? TypeCasting.to(
                        new TypeDescription.ForLoadedType(signature.trackerT().getRawType()))
                    : StackManipulation.Trivial.INSTANCE));
      }
      return new StackManipulation.Compound(parameters);
    }

    @Override
    protected StackManipulation afterDelegation(MethodDescription instrumentedMethod) {
      if (TypeDescription.VOID.equals(targetMethod.getReturnType().asErasure())) {
        return new StackManipulation.Compound(
            MethodInvocation.invoke(PROCESS_CONTINUATION_STOP_METHOD), MethodReturn.REFERENCE);
      } else {
        return MethodReturn.returning(targetMethod.getReturnType().asErasure());
      }
    }
  }

  private static class UserCodeMethodInvocation implements StackManipulation {

    @Nullable private final Integer returnVarIndex;
    private final MethodDescription targetMethod;
    private final MethodDescription instrumentedMethod;
    private final TypeDescription returnType;

    private final Label wrapStart = new Label();
    private final Label wrapEnd = new Label();
    private final Label tryBlockStart = new Label();
    private final Label tryBlockEnd = new Label();
    private final Label catchBlockStart = new Label();
    private final Label catchBlockEnd = new Label();

    private final MethodDescription createUserCodeException;

    UserCodeMethodInvocation(
        @Nullable Integer returnVarIndex,
        MethodDescription targetMethod,
        MethodDescription instrumentedMethod) {
      this.returnVarIndex = returnVarIndex;
      this.targetMethod = targetMethod;
      this.instrumentedMethod = instrumentedMethod;
      this.returnType = targetMethod.getReturnType().asErasure();

      boolean targetMethodReturnsVoid = TypeDescription.VOID.equals(returnType);
      checkArgument(
          (returnVarIndex == null) == targetMethodReturnsVoid,
          "returnVarIndex should be defined if and only if the target method has a return value");

      try {
        createUserCodeException =
            new MethodDescription.ForLoadedMethod(
                UserCodeException.class.getDeclaredMethod("wrap", Throwable.class));
      } catch (NoSuchMethodException | SecurityException e) {
        throw new RuntimeException("Unable to find UserCodeException.wrap", e);
      }
    }

    @Override
    public boolean isValid() {
      return true;
    }

    private Object describeType(Type type) {
      switch (type.getSort()) {
        case Type.OBJECT:
          return type.getInternalName();
        case Type.INT:
        case Type.BYTE:
        case Type.BOOLEAN:
        case Type.SHORT:
          return Opcodes.INTEGER;
        case Type.LONG:
          return Opcodes.LONG;
        case Type.DOUBLE:
          return Opcodes.DOUBLE;
        case Type.FLOAT:
          return Opcodes.FLOAT;
        default:
          throw new IllegalArgumentException("Unhandled type as method argument: " + type);
      }
    }

    private void visitFrame(
        MethodVisitor mv, boolean localsIncludeReturn, @Nullable String stackTop) {
      boolean hasReturnLocal = (returnVarIndex != null) && localsIncludeReturn;

      Type[] localTypes = Type.getArgumentTypes(instrumentedMethod.getDescriptor());
      Object[] locals = new Object[1 + localTypes.length + (hasReturnLocal ? 1 : 0)];
      locals[0] = instrumentedMethod.getReceiverType().asErasure().getInternalName();
      for (int i = 0; i < localTypes.length; i++) {
        locals[i + 1] = describeType(localTypes[i]);
      }
      if (hasReturnLocal) {
        locals[locals.length - 1] = returnType.getInternalName();
      }

      Object[] stack = stackTop == null ? new Object[] {} : new Object[] {stackTop};

      mv.visitFrame(Opcodes.F_NEW, locals.length, locals, stack.length, stack);
    }

    @Override
    public Size apply(MethodVisitor mv, Context context) {
      Size size = new Size(0, 0);

      mv.visitLabel(wrapStart);

      String throwableName = new TypeDescription.ForLoadedType(Throwable.class).getInternalName();
      mv.visitTryCatchBlock(tryBlockStart, tryBlockEnd, catchBlockStart, throwableName);

      // The try block attempts to perform the expected operations, then jumps to success
      mv.visitLabel(tryBlockStart);
      size = size.aggregate(MethodInvocation.invoke(targetMethod).apply(mv, context));

      if (returnVarIndex != null) {
        mv.visitVarInsn(Opcodes.ASTORE, returnVarIndex);
        size = size.aggregate(new Size(-1, 0)); // Reduces the size of the stack
      }
      mv.visitJumpInsn(Opcodes.GOTO, catchBlockEnd);
      mv.visitLabel(tryBlockEnd);

      // The handler wraps the exception, and then throws.
      mv.visitLabel(catchBlockStart);
      // In catch block, should have same locals and {Throwable} on the stack.
      visitFrame(mv, false, throwableName);

      // Create the user code exception and throw
      size =
          size.aggregate(
              new Compound(MethodInvocation.invoke(createUserCodeException), Throw.INSTANCE)
                  .apply(mv, context));

      mv.visitLabel(catchBlockEnd);

      // After the catch block we should have the return in scope, but nothing on the stack.
      visitFrame(mv, true, null);

      // After catch block, should have same locals and will have the return on the stack.
      if (returnVarIndex != null) {
        mv.visitVarInsn(Opcodes.ALOAD, returnVarIndex);
        size = size.aggregate(new Size(1, 0)); // Increases the size of the stack
      }
      mv.visitLabel(wrapEnd);
      if (returnVarIndex != null) {
        // Drop the return type from the locals
        mv.visitLocalVariable(
            "res",
            returnType.getDescriptor(),
            returnType.getGenericSignature(),
            wrapStart,
            wrapEnd,
            returnVarIndex);
      }

      return size;
    }
  }

  /**
   * A constructor {@link Implementation} for a {@link DoFnInvoker class}. Produces the byte code
   * for a constructor that takes a single argument and assigns it to the delegate field.
   */
  private static final class InvokerConstructor implements Implementation {
    @Override
    public InstrumentedType prepare(InstrumentedType instrumentedType) {
      return instrumentedType;
    }

    @Override
    public ByteCodeAppender appender(final Target implementationTarget) {
      return new ByteCodeAppender() {
        @Override
        public Size apply(
            MethodVisitor methodVisitor,
            Context implementationContext,
            MethodDescription instrumentedMethod) {
          StackManipulation.Size size =
              new StackManipulation.Compound(
                      // Load the this reference
                      MethodVariableAccess.REFERENCE.loadOffset(0),
                      // Invoke the super constructor (default constructor of Object)
                      MethodInvocation.invoke(
                          new TypeDescription.ForLoadedType(Object.class)
                              .getDeclaredMethods()
                              .filter(
                                  ElementMatchers.isConstructor()
                                      .and(ElementMatchers.takesArguments(0)))
                              .getOnly()),
                      // Load the this reference
                      MethodVariableAccess.REFERENCE.loadOffset(0),
                      // Load the delegate argument
                      MethodVariableAccess.REFERENCE.loadOffset(1),
                      // Assign the delegate argument to the delegate field
                      FieldAccess.forField(
                              implementationTarget
                                  .getInstrumentedType()
                                  .getDeclaredFields()
                                  .filter(ElementMatchers.named(FN_DELEGATE_FIELD_NAME))
                                  .getOnly())
                          .putter(),
                      // Return void.
                      MethodReturn.VOID)
                  .apply(methodVisitor, implementationContext);
          return new Size(size.getMaximalSize(), instrumentedMethod.getStackSize());
        }
      };
    }
  }
}
