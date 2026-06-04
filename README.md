# Taller12B

# Instalación de Spark en GitHub Codespaces

---

## Creación del codespace

- Para comenzar, ingresa a la página de GitHub Codespaces y crea un nuevo [codespace](https://github.com/codespaces).
- Una vez dentro, selecciona la plantilla **Blank** y espera a que el entorno termine de iniciarse.

## Instalación de los lenguajes de programación

Desde la terminal de tu codespace, utilizarás la herramienta **sdkman** para instalar *Scala (3.7.3)* y *Java (17.0.19)*. Estos son los comandos que debes ejecutar:

Para instalar **Scala**:

```bash
sdk install scala 3.7.3
```

Para instalar **Java**:

```bash
sdk install java 17.0.19-tem
```

Cuando finalice la instalación de Java, presiona la tecla **Y** para establecer la versión 17 como la versión predeterminada.

## Instalación de las herramientas

A continuación, instalarás las herramientas necesarias para trabajar con **Spark** a través de **Zeppelin**.

Primero descargarás *Spark 3.5.1 con Hadoop 3*. Para ello, ejecuta el siguiente comando en la terminal de tu codespace:

```bash
curl https://archive.apache.org/dist/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz -o spark
```

Este comando descarga aproximadamente **382 MB**, por lo que puede tardar un poco en completarse. Cuando la descarga termine, descomprime el archivo con el siguiente comando:

```bash
tar -xzf spark
```

Al descomprimirlo se creará la carpeta *spark-3.5.1-bin-hadoop3*, que podrás ver en el panel izquierdo de archivos de **VS Code**.

Ahora es el turno de instalar **Apache Zeppelin**. Ejecuta el siguiente comando en la terminal:

```bash
curl https://dlcdn.apache.org/zeppelin/zeppelin-0.12.0/zeppelin-0.12.0-bin-all.tgz -o zeppelin
```

Enseguida, descomprime el archivo:

```bash
tar -xzf zeppelin
```

Este comando crea la carpeta *zeppelin-0.12.0-bin-all*, que también aparecerá en el panel izquierdo de archivos de **VS Code**.

## Arrancar Zeppelin

Ya tienes instalado todo lo necesario para arrancar Zeppelin. En la terminal de tu codespace, ejecuta:

```bash
cd zeppelin-0.12.0-bin-all
bin/zeppelin-daemon.sh start
```

Esto pondrá en marcha el *daemon* de Zeppelin en segundo plano. Deberías ver un mensaje de confirmación en la terminal.

## Acceder a Zeppelin

Para acceder a Zeppelin necesitas abrir el **puerto 8080** en la configuración del codespace. Junto a la pestaña de la terminal encontrarás la pestaña **Puertos**; haz clic en ella y luego en el botón **Agregar puerto**, escribe `8080` y presiona **Enter**.

Con esto, el puerto quedará disponible y podrás abrir Zeppelin en el navegador mediante la URL que aparece en la columna **Dirección reenviada**. Coloca el puntero del mouse sobre la URL y haz clic en el ícono del *globo*: se abrirá una nueva pestaña en tu navegador con la interfaz de Zeppelin.

## Configurar Zeppelin

Debes indicarle a Zeppelin dónde se encuentra el directorio de datos de Spark. Sigue estos pasos:

1. En la esquina superior derecha, abre el menú que muestra el texto **anonymous** y selecciona la opción **Interpreter**.
2. En la nueva pantalla, usa el cuadro de búsqueda de intérpretes (lo identificarás por el título *Interpreters / Manage interpreters...*) y escribe `spark`. Esto mostrará el intérprete de Spark que debes editar.
3. Haz clic en el botón **Edit** para entrar en modo edición.
4. Modifica la propiedad **SPARK_HOME** con la ruta del directorio de Spark.

> Para obtener esa ruta, regresa a la pestaña del codespace, ubica la carpeta *spark-3.5.1-bin-hadoop3* en el panel izquierdo, haz clic derecho sobre ella y elige la opción **Copiar ruta de acceso**. Luego vuelve a Zeppelin y pega la ruta en la propiedad **SPARK_HOME**.

5. Desplázate hasta el final de las opciones de configuración del intérprete y haz clic en el botón **Save**.

## Crear un nuevo notebook

Puedes volver a la página principal de Zeppelin (haciendo clic en el ícono de la esquina superior izquierda) o usar el menú **Notebook**. Con cualquiera de estas dos opciones, haz clic en el botón **Create a new Note**.

En la ventana emergente que aparece, borra el contenido del campo **Create** y escribe: `/paa-aa26/taller-1`. Verifica además que el combo **Default interpreter** tenga seleccionada la opción **spark**.

## Verificar la configuración

En el párrafo de código que aparece por defecto en el nuevo notebook, escribe lo siguiente:

```scala
1 + 1
```

Ejecútalo con la combinación de teclas **Shift + Enter** o con el botón de *play* que aparece en la esquina superior derecha del párrafo.

Si todo salió bien, verás el resultado de la operación (`res1: Int = 2`) en la parte inferior del párrafo. Si tienes algún problema, revisa nuevamente el proceso de instalación.

# Contar palabras

Tal como estudiamos en clase, ahora agregarás el siguiente código para contar las palabras de la novela *Don Quijote de la Mancha*. Sigue estos pasos:

El primer paso es subir el archivo *elquijote.txt* a tu codespace. Ve a tu navegador de archivos, selecciona *elquijote.txt* y **arrástralo hasta la sección de archivos del codespace** para soltarlo allí; con esto el archivo quedará alojado en la nube del codespace.

### Párrafo 1 — Cargar los datos (RDD = colección distribuida)
```scala
val ruta = "file:///home/zeppelin/datos/elquijote.txt"  // <-- AJUSTA esta ruta
val lineas = sc.textFile(ruta)
println(s"Líneas leídas: ${lineas.count()}")   // count() es una ACCIÓN
```

### Párrafo 2 — Transformaciones perezosas (Map + paralelismo de datos)
```scala
val palabras = lineas
  .flatMap(linea => linea.toLowerCase.split("[^\\p{L}]+"))
  .filter(_.nonEmpty)

val paresPalabraUno = palabras.map(p => (p, 1))
// Nada se ha ejecutado todavía: solo definimos la "receta".
```

### Párrafo 3 — Reduce (combinar conteos por clave)
```scala
val conteos = paresPalabraUno.reduceByKey(_ + _)
```

### Párrafo 4 — Acción: dispara el cálculo y muestra el TOP 15
```scala
println("=== TOP 15 (sin filtrar stopwords) ===")
conteos.takeOrdered(15)(Ordering.by(par => -par._2))
  .foreach { case (palabra, n) => println(f"$palabra%-12s $n") }
```

### Párrafo 5 — Palabras con significado (filtrar stopwords)
```scala
val stopwords = Set("de","la","que","el","en","y","a","los","del","se","las","por",
  "un","para","con","no","una","su","al","lo","como","más","mas","pero","sus","le",
  "ya","o","este","sí","si","porque","esta","entre","cuando","muy","sin","sobre",
  "también","me","hasta","hay","donde","quien","desde","todo","nos","todos","uno",
  "les","ni","ese","eso","ante","ellos","e","esto","mí","qué","unos","yo","otro",
  "él","tanto","esa","estos","mucho","nada","muchos","cual","poco","ella","tan",
  "así","pues","era","fue","ser","son","había")

val significativas = conteos.filter { case (p, _) => !stopwords.contains(p) && p.length > 2 }

println("=== TOP 20 CON SIGNIFICADO ===")
significativas.takeOrdered(20)(Ordering.by(par => -par._2))
  .foreach { case (palabra, n) => println(f"$palabra%-12s $n") }
```

### Párrafo 6 (opcional) — Visualizar con SQL/tabla en Zeppelin
```scala
// Pasamos a DataFrame para usar la visualización integrada de Zeppelin.
val df = significativas.toDF("palabra", "frecuencia")
df.createOrReplaceTempView("conteos")
```
```sql
%sql
SELECT palabra, frecuencia
FROM conteos
ORDER BY frecuencia DESC
LIMIT 20
```

Con la celda `%sql`, Zeppelin te permite cambiar a un *gráfico de barras* con un solo clic.

## Detener el codespace

Codespaces ofrece de forma gratuita **60 horas de uso mensual**, por lo que no conviene dejarlo activo sin necesidad. Para detenerlo, cierra la pestaña de Zeppelin, luego la pestaña del codespace donde se ejecuta VS Code y, finalmente, regresa a la página principal de GitHub Codespaces.

En la parte izquierda deberías ver un menú con la etiqueta **By repository** (si no aparece, refresca la página). En la parte inferior de la sección central verás el texto *Owned by &lt;tu usuario de GitHub&gt;* junto al nombre del codespace que utilizaste. Haz clic en los **tres puntos** de la derecha y realiza estas dos acciones:

- Haz clic en la opción **Stop codespace**.
- Luego, haz clic en **Auto-delete codespace**, para evitar que se elimine automáticamente después de cierto tiempo.

De esta manera termina este taller individual, recuerda mostrar el resultado de tu trabajo a tu tutor para que registre tu participación.
