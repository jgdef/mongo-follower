package com.traackr.mongo.follower.service;

import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import net.helenus.mapping.annotation.InheritedTable;

import org.jooq.lambda.Seq;
import org.jooq.lambda.Unchecked;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.io.support.ResourcePatternResolver;
import org.springframework.core.type.classreading.CachingMetadataReaderFactory;
import org.springframework.core.type.classreading.MetadataReader;
import org.springframework.core.type.classreading.MetadataReaderFactory;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import com.belmonttech.MongoMigrationSetter;
import com.google.common.collect.ImmutableMap;
import com.traackr.mongo.follower.model.OplogEntry;

public class CassandraSink {
  private static final Logger logger = Logger.getLogger(CassandraSink.class.getName());
  private static final String MODEL_OBJECT_PACKAGE = "com.belmonttech.model";
  private static final Class<?> CASSANDRA_ENTITY_DRAFT_CLASS;
  private static final MetadataReaderFactory METADATA_READER_FACTORY;
  private static final Class<?>[] NOARG_PARAMS = new Class<?>[0];

  private static ImmutableMap<String, Class<?>> namespaceClassMap;
  private static ImmutableMap<String, Method> fieldSetterMap;
  static {
      try {
          CASSANDRA_ENTITY_DRAFT_CLASS = Class.forName("com.belmonttech.model.BTAbstractEntityDraft");
          METADATA_READER_FACTORY = new CachingMetadataReaderFactory(CassandraSink.class.getClassLoader());
          setup();
      } catch(Exception e) {
          throw new ExceptionInInitializerError(e);
      }
  }

  public CassandraSink() {}

  /**
   * Since this is invoked at class loading time and specifically in the static block,
   * any throwable is converted to {@link ExceptionInInitializerError}.
   * @throws Exception
   */
  private static void setup() throws Exception{
      ResourcePatternResolver patternResolver = new PathMatchingResourcePatternResolver();

      // Possible fix - change Cassandra model object package name to be unique.
      String searchPath = ResourcePatternResolver.CLASSPATH_ALL_URL_PREFIX
        + ClassUtils.convertClassNameToResourcePath(MODEL_OBJECT_PACKAGE) + "/**/*.class";

      List<Class<?>> cassandraDraftClasses = Arrays.asList(patternResolver.getResources(searchPath))
        .stream()
        .filter(Unchecked.predicate(CassandraSink::isCandidateResource))
        .map(Unchecked.function(CassandraSink::resourceToClass))
        .collect(Collectors.toList());
      logger.info("Number of discovered Cassandra classes: " + cassandraDraftClasses.size());

      Map<String, Method> methodMap = Seq.seq(cassandraDraftClasses)
          .map(CassandraSink::collectAnnotatedMethods)
          .flatMap(List::stream)
          .collect(Collectors.toMap(m -> {
              Annotation a = m.getAnnotation(MongoMigrationSetter.class);
              Assert.notNull(a, "Method should have the MongoMigrationSetter annotation: " + m.getName());

              MongoMigrationSetter mm = (MongoMigrationSetter) a;
              Assert.hasText(mm.database(), "Mongo annotation database attribute is blank");
              Assert.hasText(mm.collection(), "Mongo annotation collection attribute is blank");
              Assert.hasText(mm.field(), "Mongo annotation field attribute is blank");
              return mm.database() + "." + mm.collection() + "." + mm.field();
          }, Function.identity()));
      fieldSetterMap = ImmutableMap.copyOf(methodMap);

      Map<String, Class<?>> classMap = Seq.seq(cassandraDraftClasses)
          .map(CassandraSink::collectAnnotatedMethods)
          .flatMap(List::stream)
          .groupBy(m -> {
              Annotation a = m.getAnnotation(MongoMigrationSetter.class);
              Assert.notNull(a, "Method should have the MongoMigrationSetter annotation: " + m.getName());

              MongoMigrationSetter mm = (MongoMigrationSetter) a;
              return mm.database() + "." + mm.collection();
          }, Collectors.collectingAndThen(Collectors.toSet(),
              set -> set.stream().findAny().get().getDeclaringClass()));
      namespaceClassMap = ImmutableMap.copyOf(classMap);
  }

  private static boolean isCandidateResource(Resource resource) throws Exception {
      if (!resource.isReadable()) {
          logger.warning("Unreadable resource " + resource);
          return false;
      }

      Class<?> cls = resourceToClass(resource);
      Class<?> parent = cls.getEnclosingClass();
      String clsName = cls.getName();

      if (parent == null) {
          logger.fine("Not an inner class; rejecting " + clsName);
          return false;
      }

      if (parent.getAnnotation(InheritedTable.class) == null) {
          logger.fine("No @InheritedTable annotation on parent class; rejecting " + clsName);
          return false;
      }

      if (!Modifier.isStatic(cls.getModifiers())) {
          logger.fine("Not a static class; rejecting " + clsName);
          return false;
      }

      if (!CASSANDRA_ENTITY_DRAFT_CLASS.isAssignableFrom(cls)) {
          logger.fine("Not an entity draft; rejecting " + clsName);
          return false;
      }

      // Too paranoid?
      if (cls.getTypeParameters() == null) {
          logger.fine("Entity draft has no type parameters; rejecting " + clsName);
          return false;
      }

      logger.fine("Found candidate " + clsName);
      return true;
  }

  private static Class<?> resourceToClass(Resource resource) throws Exception {
      // This is run after {@link isCandidate}, so class information is already cached.
      // CachingMetadataReaderFactory is thread-safe.
      MetadataReader metadataReader = METADATA_READER_FACTORY.getMetadataReader(resource);
      return Class.forName(metadataReader.getClassMetadata().getClassName());
  }

  private static List<Method> collectAnnotatedMethods(Class <?> cls) {
      return Arrays.asList(cls.getDeclaredMethods())
          .stream()
          .filter(m -> m.getAnnotation(MongoMigrationSetter.class) != null)
          .collect(Collectors.toList());
  }

  public static void process(OplogEntry oplogEntry) throws Exception {
      String ns = oplogEntry.getNamespace();
      Assert.hasText(ns, "Null oplog entry namespace");

      Map<String, Object> entry = oplogEntry.getRawEntry();
      Object op = entry.get(OplogEntry.OPLOG_FIELD_TYPE);
      Assert.notNull(entry, "Null oplog entry document");
      Assert.notNull(op, "Null oplog type code");

      // Has the mongo model object has been converted to a Cassandra draft object yet?
      if (!namespaceClassMap.containsKey(ns)) {
          logger.warning("No cassandra entity corresponding to oplog entry namespace " + ns);
          return;
      }

      // Not all oplog entries have an associated document, but the entries
      // that we're interested in should.
      if (!entry.containsKey(OplogEntry.OPLOG_FIELD_DOC)) {
          logger.warning("Null oplog document field");
          return;
      }

      @SuppressWarnings("unchecked")
      Map<String, Object> doc = (Map<String, Object>) entry.get(OplogEntry.OPLOG_FIELD_DOC);
      Class<?> entityClass = namespaceClassMap.get(ns);
      Constructor<?> ctor = entityClass.getDeclaredConstructor(NOARG_PARAMS);
      ctor.setAccessible(true);

      // Iterate through all entries in the oplog document, find setters, and invoke on the draft object.
      Object draft = ctor.newInstance();
      doc.entrySet().stream().forEach(e -> {
          String fieldName = ns + "." + e.getKey();
          Method setter = fieldSetterMap.get(fieldName);
          if (setter == null) {
              logger.warning("No setter method found for field " + fieldName);
              return;
          }

          Object value = getSetterValue(setter, e.getValue());
          if (value == null) {
              logger.warning("How to handle null values?");
              return;
          }
          try {
              setter.invoke(draft, value);
          } catch (InvocationTargetException | IllegalAccessException invokeException) {
              invokeException.printStackTrace(System.err);
          }

          logger.info("Invoked setter on draft object for field " + fieldName);
      });

      logger.info("All setters invoked on draft object " + draft);
  }

  private static Object getSetterValue(Method setter, Object rawValue) {
      // Hack for enum transforms... make it better... a List of transform functions?
      Class<?>[] setterArgTypes = setter.getParameterTypes();
      Assert.notNull(setterArgTypes, "Setter methods can't be no-arg methods");
      Assert.isTrue(setterArgTypes.length == 1, "Setter methods should have only one argument");

      if (setterArgTypes[0].isEnum()) {
          MongoMigrationSetter annotation = setter.getAnnotation(MongoMigrationSetter.class);
          Assert.notNull(annotation, "No mongo setter annotation found");

          if ("enum".equals(annotation.translation())) {
              @SuppressWarnings("unchecked")
              Class<? extends Enum<?>> eClass = (Class<? extends Enum<?>>) setterArgTypes[0];
              Class<?> valueClass = rawValue.getClass();
              Assert.isTrue(
                    Integer.class.isAssignableFrom(valueClass) || int.class.isAssignableFrom(valueClass),
                    "Expected integer argument to enum setter method");
              // TODO: will have to check value is in the bounds of the enum; there may
              // be new values added in different versions, etc.
              rawValue = eClass.getEnumConstants()[(int)rawValue];
          }
      }

      return rawValue;
  }
}
