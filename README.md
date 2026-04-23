## 🚀 Spark & Scala: Big Data Processing Solutions

Este repositorio contiene la implementación técnica de una serie de ejercicios avanzados de procesamiento de datos utilizando Apache Spark y Scala. El proyecto abarca desde la manipulación de datos estructurados hasta la implementación de lógica personalizada y pruebas unitarias para garantizar la calidad del código.

🛠️ Tecnologías y Conceptos Clave
- Lenguaje: Scala 2.12+.
- Framework: Apache Spark 3.x.
- Testing: Implementación de pruebas unitarias con ScalaTest para validar la lógica de las transformaciones.
- Técnicas de Ingeniería: Joins, Agregaciones masivas, UDFs (User Defined Functions) y operaciones de ventana.

📘 Contenido del Repositorio

El proyecto se divide en dos componentes principales:

1. Núcleo de Procesamiento (Examen.scala)
Contiene la lógica de negocio distribuida, organizada en los siguientes módulos:

Operaciones Básicas: Filtrado dinámico, proyecciones y ordenación de datos académicos.

Lógica Personalizada (UDF): Implementación de funciones de usuario para la clasificación de datos en tiempo real.

Relaciones Complejas: Join de DataFrames de estudiantes y calificaciones para el cálculo de promedios ponderados.

Arquitectura RDD: Implementación de un flujo de trabajo para el conteo de frecuencias (WordCount) mediante el paradigma funcional.

ETL de Ventas: Pipeline para calcular ingresos totales (cantidad * precio_unitario) con agrupaciones por producto.

2. Validación y Calidad (examenTest.scala)
Este archivo contiene la suite de pruebas que asegura que cada transformación de Spark produzca los resultados esperados.

Validación de esquemas.

Pruebas de integridad para los Joins.

Verificación de la lógica de las UDFs y los acumuladores en RDDs.

⚙️ Configuración del Entorno
Requisitos:
- Java JDK 8 o 11.
- Scala SDK.
- SBT (Scala Build Tool).

📂 Estructura de Archivos
- Examen.scala: Contiene las funciones de transformación y procesamiento.
- examenTest.scala: Suite de tests unitarios utilizando Matchers y TestInit.
