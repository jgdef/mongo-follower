package com.traackr.mongo.follower.service;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import net.helenus.core.HelenusSession;
import net.helenus.core.operation.InsertOperation;
import net.helenus.core.reflect.Entity;
import net.helenus.mapping.annotation.ClusteringColumn;
import net.helenus.mapping.annotation.InheritedTable;
import net.helenus.mapping.annotation.PartitionKey;

import org.jooq.lambda.tuple.Tuple2;
import org.jooq.lambda.Seq;
import org.jooq.lambda.Unchecked;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.io.support.ResourcePatternResolver;
import org.springframework.core.type.classreading.CachingMetadataReaderFactory;
import org.springframework.core.type.classreading.MetadataReader;
import org.springframework.core.type.classreading.MetadataReaderFactory;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import com.belmonttech.MongoMigrationSetter;
import com.belmonttech.model.BTAbstractEntityDraft;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.traackr.mongo.follower.model.OplogEntry;
import com.traackr.mongo.follower.model.Update;

public class CassandraSink {
  private static final Logger LOG = LoggerFactory.getLogger(CassandraSink.class);
  private static final String MODEL_OBJECT_DSL_FIELD = "dsl";
  private static final String MODEL_OBJECT_DRAFT_CREATE_METHOD = "draft";
  private static final String MODEL_OBJECT_DRAFT_UPDATE_METHOD = "update";
  private static final String MODEL_OBJECT_PACKAGE = "com.belmonttech.model";
  private static final Class<?> CASSANDRA_ENTITY_DRAFT_CLASS;
  private static final Class<?> HELENUS_ENTITY_CLASS;
  private static final String HELENUS_ENTITY_CLASS_NAME = "net.helenus.core.reflect.Entity";
  private static final MetadataReaderFactory METADATA_READER_FACTORY;
  private static final Class<?>[] NOARG_CLASS_PARAMS = new Class<?>[0];
  private static final Object[] NOARG_OBJ_PARAMS = new Object[0];

  private static HelenusSession hs;
  
  /** Mongo oplog entry namespace to entity object constructor map. */
  /** TODO: delete */
///  private static ImmutableMap<String, Class<?>> namespaceClassMap;
  
  /**
   * Mongo oplog namespace to entity object methods to create a new (first
   * tuple member) and update (second tuple member) entity draft objects map.
   */
  private static ImmutableMap<String, Tuple2<Method, Method>> namespaceEntityMethodsMap;
  
  /** Mongo oplog namespace + field name to entity setter method map. */
  private static ImmutableMap<String, Method> fieldSetterMap;
  
  /** Entity object class to primary key columns map */
  private static ImmutableMap<Class<? extends Entity>, List<Field>> entityPrimaryKeyColumnsMap;
  static {
      try {
          CASSANDRA_ENTITY_DRAFT_CLASS = BTAbstractEntityDraft.class;
          HELENUS_ENTITY_CLASS = Class.forName(HELENUS_ENTITY_CLASS_NAME);
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
      String searchPath = ResourcePatternResolver.CLASSPATH_ALL_URL_PREFIX
        + ClassUtils.convertClassNameToResourcePath(MODEL_OBJECT_PACKAGE) + "/**/*.class";

      List<Resource> resources = Arrays.asList(patternResolver.getResources(searchPath));
      List<Class<?>> cassandraDraftClasses = Seq.seq(resources)
        .map(Unchecked.function(CassandraSink::checkAndReturnClassFromCandidateResource))
        .filter(Objects::nonNull)
        .collect(Collectors.toList());
      LOG.info("Number of discovered Cassandra classes: {}", cassandraDraftClasses.size());
 
      // Map the Mongo DB/collection/field information, found on new model object setter methods,
      // to the collected Cassandra draft clases. 
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
              Assert.notNull(a, "Method should have the MongoMigrationSetterSetter annotation: " + m.getName());

              MongoMigrationSetter mm = (MongoMigrationSetter) a;
              return mm.database() + "." + mm.collection();
          }, Collectors.collectingAndThen(Collectors.toSet(),
              set -> set.stream().findAny().get().getDeclaringClass()));
      
      Map<String, Tuple2<Method, Method>> draftMethodMap = Seq.seq(classMap.entrySet())
          .map(e -> Maps.immutableEntry(e.getKey(), getDraftMethodsFromEntityClass(e.getValue())))
          .filter(e -> e.getValue().isPresent())
          .toMap(Map.Entry::getKey, e -> e.getValue().get());
      namespaceEntityMethodsMap = ImmutableMap.copyOf(draftMethodMap);
      
      // For each entity type, there is 1...N columns that represent the primary key. They can 
      // be partitioning or clustering columns.
      @SuppressWarnings("unchecked")
      Map<Class<BTAbstractEntityDraft<?, ?>>, List<Field>> primaryKeyColumns = Seq.seq(cassandraDraftClasses)
          .map(c -> (Class<BTAbstractEntityDraft<?, ?>>) c)
          .toMap(Function.identity(), CassandraSink::collectPrimaryKeyColumns);
      //entityPrimaryKeyColumnsMap = ImmutableMap.copyOf(primaryKeyColumns);
  }

  /**
   * This method determines if a resource corresponds to an abstract Cassanrda entity class
   * ("GBTAbstract..."). From these classes, we get:
   * <ul>
   *   <li>The proxied entity class, through the "dsl" static member (this class is typically
   *       the immediate subclass of the abstract entity class).</li>
   *   <li>From above, the entity class's inner Draft static class</li>
   *   <li>From the abstract entity class's inner Draft static class, which contains the
   *       setter methods that are invoked on instance of the proxied entity draft class.</li>
   * </ul>
   * 
   */
  private static Class<?> checkAndReturnClassFromCandidateResource(Resource resource) throws Exception {
      if (!resource.isReadable()) {
          LOG.warn("Unreadable resource {}", resource);
          return null;
      }

      Class<?> cls = resourceToClass(resource);
      Class<?> parent = cls.getEnclosingClass();
      String clsName = cls.getName();

      if (parent == null) {
          LOG.debug("Not an inner class; rejecting {}", clsName);
          return null;
      }
      
      if (!HELENUS_ENTITY_CLASS.isAssignableFrom(parent)) {
          LOG.warn("Unexpected parent class type {}; rejecting {}", parent, clsName);
          return null;
      }

      if (parent.getAnnotation(InheritedTable.class) == null) {
          LOG.debug("No @InheritedTable annotation on parent class; rejecting {}", clsName);
          return null;
      }

      if (!Modifier.isStatic(cls.getModifiers())) {
          LOG.info("Not a static class; rejecting {}", clsName);
          return null;
      }

      if (!CASSANDRA_ENTITY_DRAFT_CLASS.isAssignableFrom(cls)) {
          LOG.info("Not an entity draft; rejecting {}", clsName);
          return null;
      }

      // Too paranoid?
      if (cls.getTypeParameters() == null) {
          LOG.info("Entity draft has no type parameters; rejecting {}", clsName);
          return null;
      }
      
      LOG.info("Found candidate {}", clsName);
      return cls;
  }

  private static Class<?> resourceToClass(Resource resource) throws Exception {
      // This is run after {@link isCandidate}, so class information is already cached.
      // CachingMetadataReaderFactory is thread-safe.
      MetadataReader metadataReader = METADATA_READER_FACTORY.getMetadataReader(resource);
      return Class.forName(metadataReader.getClassMetadata().getClassName());
  }

  private static List<Method> collectAnnotatedMethods(Class <?> cls) {
      List<Method> allMethods = Arrays.asList(cls.getMethods());
      List<Method> annotatedMethods = Seq.seq(allMethods)
          .filter(m -> m.getAnnotation(MongoMigrationSetter.class) != null)
          .toList();

      LOG.info("Class {} has {} annotated method(s)", cls, annotatedMethods.size());
      return annotatedMethods;
  }
  
  private static List<Field> collectPrimaryKeyColumns(Class<BTAbstractEntityDraft<?, ?>> cls) {
      List<Field> allFields = Arrays.asList(cls.getFields());
      List<Field> primaryKeyFields = Seq.seq(allFields)
          .filter(f -> f.getAnnotation(ClusteringColumn.class) != null
              || f.getAnnotation(PartitionKey.class) != null)
          .toList();
      
      LOG.info("Class {} has {} primary key column(s)", cls, primaryKeyFields.size());
      return primaryKeyFields;
  }

  public static void process(OplogEntry oplogEntry) throws Exception {
      String ns = oplogEntry.getNamespace();
      Assert.hasText(ns, "Null oplog entry namespace");

      Map<String, Object> entry = oplogEntry.getRawEntry();
      Object op = entry.get(OplogEntry.OPLOG_FIELD_TYPE);
      Assert.notNull(entry, "Null oplog entry document");
      Assert.notNull(op, "Null oplog type code");
      Assert.isInstanceOf(String.class, op);
      String opType = (String)op;

      // Has the mongo model object has been converted to a Cassandra draft object yet?
      if (!namespaceEntityMethodsMap.containsKey(ns)) {
          LOG.warn("No cassandra entity corresponding to oplog entry namespace {}", ns);
          return;
      }

      // Not all oplog entries have an associated document, but the entries
      // that we're interested in should.
      if (!entry.containsKey(OplogEntry.OPLOG_FIELD_DOC)) {
          LOG.warn("Null oplog document field");
          return;
      }
      
      Optional<? extends BTAbstractEntityDraft<?, ?>> draftOpt = getDraftFromOplogEntry(opType, oplogEntry);
      if (!draftOpt.isPresent()) {
          LOG.warn("TODO");
          return;
      }
      
      BTAbstractEntityDraft<?, ?> draft = draftOpt.get();  
      Class<?> c1 = draft.getEntityClass();
      
      
      @SuppressWarnings("unchecked")
      Map<String, Object> doc = (Map<String, Object>) entry.get(OplogEntry.OPLOG_FIELD_DOC);
      
      Map<String, Object> primaryKeyValuesMap = getPrimaryKeyValuesFromOplogEntry(c1, doc);
      
      doc.entrySet().stream().forEach(e -> {
          String fieldName = ns + "." + e.getKey();
          Method setter = fieldSetterMap.get(fieldName);
          if (setter == null) {
              LOG.warn("No setter method found for field {}", fieldName);
              return;
          }

          Object value = getSetterValue(setter, e.getValue());
          if (value == null) {
              LOG.warn("How to handle null values?");
              return;
          }
          try {
              setter.invoke(draft, value);
          } catch (InvocationTargetException | IllegalAccessException invokeException) {
              invokeException.printStackTrace(System.err);
          }

          LOG.debug("Invoked setter on draft object for field {}", fieldName);
      });

      LOG.info("All setters invoked on draft object {}", draft);


      InsertOperation<?> ret = hs.upsert(draft);
      
      // If this is an update, then we need a map of primary key field to 
      // primary key value. Note this can be primary key and clustering 
      // columns.
      Map<?, ?> primaryKey;
  }

  private static Optional<Tuple2<Method, Method>> getDraftMethodsFromEntityClass(Class<?> cls) {
      // Find the entity object proxied by the dsl field. If this returns 
      // the field can't be null (a NoSuchFieldException would have been thrown).
      //
      // The relationship between the entity class and the proxied class is:
      //   The entity class has a dsl field that is a proxy to the proxied class
      //   The proxied class extends the entity class' parent class.
      Method newDraft, updateDraft;
      try {
          Field dsl = cls.getEnclosingClass().getField(MODEL_OBJECT_DSL_FIELD);
          Class<?> dslType = dsl.getType();
          Assert.isTrue(dslType.isInterface(), "DSL type is not an interface for " + cls.getEnclosingClass().getName());
      
          newDraft = dslType.getMethod(MODEL_OBJECT_DRAFT_CREATE_METHOD, NOARG_CLASS_PARAMS);
          if ((newDraft.getModifiers() & Modifier.STATIC) == 0) {
              LOG.warn("No static method found to create draft for class {}", cls);
              return Optional.empty();
          }
      
          updateDraft = dslType.getMethod(MODEL_OBJECT_DRAFT_UPDATE_METHOD, NOARG_CLASS_PARAMS);
          if ((updateDraft.getModifiers() & Modifier.STATIC) != 0) {
              LOG.warn("No instance method found to update draft for class {}", cls);
              return Optional.empty();
          }
      } catch (ReflectiveOperationException e) {
          LOG.warn("Error scanning entity method for draft methods", e);
          return Optional.empty();
      }
      
      return Optional.of(new Tuple2<Method, Method>(newDraft, updateDraft));
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
  
    private static Optional<? extends BTAbstractEntityDraft<?, ?>> getDraftFromOplogEntry(String opLogType,
            OplogEntry entry) {
      Assert.notNull(opLogType, "Null OpLogType argunent");
      String namespace = entry.getNamespace();
      Method method = null;
      Object[] args;
      
      switch(opLogType) {
      case OplogEntry.OPLOG_INSERT:
          LOG.trace("Processing an insert oplog entry");
          method = namespaceEntityMethodsMap.get(namespace).v1();
          break;
          
      case OplogEntry.OPLOG_UPDATE:
          // If an update, the OplogEntry          
          Assert.isAssignable(Update.class, entry.getClass());
          Update update = (Update) entry;
          Map<String, Object> filter = update.getQuery();
          if (filter == null) {
              LOG.warn("No filter for oplog update operation; can't process entry");
              return Optional.empty();
          }

          args = new Object[] {ImmutableMap.of(update.getId(), filter)};
          LOG.trace("Procesing an update oplog entry");
          method = namespaceEntityMethodsMap.get(namespace).v2();
          break;
          
      case OplogEntry.OPLOG_DELETE:
          method = null;
          break;
      }
      
      if (method == null) {
          LOG.debug("Null method for oplog type {}", opLogType);
          return Optional.empty();
      }
      
      try {
          // For insert operations, the "create new entity" method is a no-arg static method,
          // so the arguments to invoke() are both null.
          BTAbstractEntityDraft<?, ?> o = (BTAbstractEntityDraft<?, ?>) method.invoke(null, null);
          return Optional.of(o);
      } catch (IllegalAccessException | InvocationTargetException e) {
          return Optional.empty();
      }
  }
    
  /** Only call for insert operations */
  private static Map<String, Object> getPrimaryKeyValuesFromOplogEntry(Class<?> c, Map<String, Object> doc) { 
      Assert.notNull(doc);
      List<Field> fields = entityPrimaryKeyColumnsMap.get(c);
      return null;
  }
}
