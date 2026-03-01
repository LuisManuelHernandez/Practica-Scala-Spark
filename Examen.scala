package examen_estructura

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._ // Necesario para funciones como filter, avg, sum, udf, col

object Examen {

  /** Ejercicio 1: Crear un DataFrame y realizar operaciones básicas
   * Muestra el esquema, filtra calif > 8, selecciona nombres y ordena por calif.
   */
  def ejercicio1(estudiantes: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    estudiantes.printSchema()
    val calificados = estudiantes.filter($"calificacion" > 8)
    calificados.show()
    val nombres = estudiantes.select("nombre","calificacion").orderBy(desc("calificacion"))
    nombres.select("nombre")
  }

  // ----------------------------------------------------------------------------------

  /** Ejercicio 2: UDF (User Defined Function)
   * Define una función que determina si un número es par o impar y la aplica.
   */
  def ejercicio2(numeros: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    val pares = udf((n: Int) => if (n % 2 == 0) "Par" else "Impar")
    val reemplazarValores = numeros.withColumn("parOImpar", pares($"numero"))
    reemplazarValores.select("parOImpar")
  }

  // ----------------------------------------------------------------------------------

  /** Ejercicio 3: Joins y agregaciones
   * Realiza un join y calcula el promedio de calificaciones por estudiante.
   */
  def ejercicio3(estudiantes: DataFrame, calificaciones: DataFrame): DataFrame = {
    import estudiantes.sparkSession.implicits._
    val estudiantesConCalificaciones = estudiantes.join(calificaciones, estudiantes("id") === calificaciones("id_estudiante"))
    val promedio = estudiantesConCalificaciones.groupBy("id","nombre").agg(avg("calificacion").alias("promedio"))
    promedio
  }

  // ----------------------------------------------------------------------------------

  /** Ejercicio 4: Uso de RDDs
   * Crea un RDD a partir de una lista de palabras y cuenta la cantidad de ocurrencias de cada palabra.
   */
  def ejercicio4(palabras: List[String])(implicit spark: SparkSession): RDD[(String, Int)] = {
    val rdd = spark.sparkContext.parallelize(palabras)
    val numeroDeVeces = rdd.map(palabra => (palabra,1)).groupByKey().mapValues(_.size).sortBy(_._2)
    numeroDeVeces
  }

  // ----------------------------------------------------------------------------------

  /**
   * Ejercicio 5: Procesamiento de archivos
   * Carga un archivo CSV y calcula el ingreso total (cantidad * precio_unitario) por producto.
   */
  def ejercicio5(ventas: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    val ingresoTotal = ventas.withColumn("ingreso", $"cantidad" * $"precio_unitario").groupBy("id_producto").agg(sum("ingreso").alias("total"))
    ingresoTotal
  }
}