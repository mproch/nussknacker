package pl.touk.nussknacker.engine.sql

import java.sql.{PreparedStatement, ResultSetMetaData, Types}
import java.util.{Collections, Properties}

import com.typesafe.scalalogging.LazyLogging
import org.apache.calcite.adapter.java.{AbstractQueryableTable, ReflectiveSchema}
import org.apache.calcite.linq4j.{EnumerableDefaults, Linq4j, QueryProvider, Queryable}
import org.apache.calcite.rel.`type`._
import org.apache.calcite.sql.`type`.{BasicSqlType, SqlTypeName}
import org.apache.calcite.sql.dialect.OracleSqlDialect
import pl.touk.nussknacker.engine.api.typed.typing.TypedObjectTypingResult
import pl.touk.nussknacker.engine.api.typed.{ClazzRef, TypedMap, TypedObjectDefinition, typing}
import pl.touk.nussknacker.engine.sql.CalciteQueryableDataBase.ColumnModelTable
import java.sql.{Connection, DriverManager}
import java.util

import org.apache.calcite.jdbc.{CalciteConnection, JavaTypeFactoryImpl}
import org.apache.calcite.schema.SchemaPlus

import scala.collection.JavaConverters._
import scala.collection.mutable

class CalciteQueryableDataBase(query: String, tables: Map[String, ColumnModel]) extends SqlQueryableDataBase with LazyLogging {

  Class.forName("org.apache.calcite.jdbc.Driver")
  val info = new Properties()
  val connection: Connection = DriverManager.getConnection("jdbc:calcite:", info)
  val calciteConnection: CalciteConnection = connection.unwrap(classOf[CalciteConnection])
  val rootSchema: SchemaPlus = calciteConnection.getRootSchema

  var tableContents: Map[String, Table] = _

  {
    tables.foreach {
      case (name, model) =>
        rootSchema.add(name.toUpperCase, new ColumnModelTable(model, () => tableContents(name)))
    }
  }

  private lazy val queryStatement = connection.prepareStatement(query)

  override def getTypingResult: typing.TypingResult = {
    val metaData = queryStatement.getMetaData
    TypedObjectTypingResult(toTypedMapDefinition(metaData))
  }

  override def query(tables: Map[String, Table]): List[TypedMap] = {
    tableContents = tables
    executeQuery(queryStatement)
  }


  private def executeQuery(statement: PreparedStatement): List[TypedMap] = {
    val rs = statement.executeQuery()
    val result = mutable.Buffer[TypedMap]()
    val types = toTypedMapDefinition(statement.getMetaData)
    while (rs.next()) {
      var map = Map[String, Any]()
      types.fields.foreach { case (k, typeRef) =>
        //FIXME: type conversions...
        map = map + (k -> rs.getObject(k))
      }
      result += TypedMap(map)
    }
    result.toList
  }

  private def toTypedMapDefinition(meta: ResultSetMetaData): TypedObjectDefinition = {
    val cols = (1 to meta.getColumnCount).map { idx =>
      val name = meta.getColumnLabel(idx)
      //more mappings?
      val typ = meta.getColumnType(idx) match {
        case Types.BIT | Types.TINYINT | Types.SMALLINT | Types.INTEGER
             | Types.BIGINT | Types.FLOAT | Types.REAL | Types.DOUBLE | Types.NUMERIC | Types.DECIMAL =>
          ClazzRef[Number]
        case Types.VARCHAR | Types.LONGNVARCHAR =>
          ClazzRef[String]
        case Types.CHAR =>
          ClazzRef[Char]
        case Types.BOOLEAN =>
          ClazzRef[Boolean]
        case Types.DATE | Types.TIMESTAMP =>
          ClazzRef[java.util.Date]
        case a =>
          logger.warn(s"no type mapping for column type: $a, column: ${meta.getColumnName(idx)}")
          ClazzRef[Any]
      }
      name -> typ
    }.toMap

    TypedObjectDefinition(cols)
  }

  override def close(): Unit = {}
}

object CalciteQueryableDataBase extends SqlQueryableDataBaseFactory {

  override def create(query: String, tables: Map[String, ColumnModel]): SqlQueryableDataBase = new CalciteQueryableDataBase(query, tables)

  class ColumnModelTable(model: ColumnModel, rows: () => Table) extends AbstractQueryableTable(classOf[Array[Object]]) {

    private val typeSystem = OracleSqlDialect.DEFAULT.getTypeSystem

    override def asQueryable[T](queryProvider: QueryProvider, schema: SchemaPlus, tableName: String): Queryable[T] = {
      EnumerableDefaults.asOrderedQueryable(Linq4j.asEnumerable(
        rows().rows.map(row => row.toArray[Any].asInstanceOf[T])
      .asJava))
    }

    override def getRowType(typeFactory: RelDataTypeFactory): RelDataType = {
      val columns: List[RelDataTypeField] = model.columns.zipWithIndex.map { case (c, idx) =>
        new RelDataTypeFieldImpl(c.name.toUpperCase, idx, new BasicSqlType(typeSystem, mapType(c.typ)))
      }
      RelDataTypeImpl.proto(new RelRecordType(columns.asJava)).apply(typeFactory)
    }

    private def mapType(typ: SqlType): SqlTypeName = typ match {
      case SqlType.Numeric => SqlTypeName.DOUBLE
      case SqlType.Decimal => SqlTypeName.DECIMAL
      case SqlType.Date => SqlTypeName.TIMESTAMP
      case SqlType.Varchar => SqlTypeName.VARCHAR
      case SqlType.Bool => SqlTypeName.BOOLEAN

    }
  }


}
