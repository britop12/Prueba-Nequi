import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
import re
import datetime
from pyspark.sql.functions import col, udf, lit
from pyspark.sql.types import StringType, IntegerType


# ==============================
# Inicialización de contexto Glue
# ==============================
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'S3_INPUT_PATH',
    'S3_OUTPUT_PATH',
    'S3_LOG_PATH',
    'RUN_DATE'
])

# Estructura de versionado en S3: processed/yyyy-mm-dd/job_run_id/
run_date = args['RUN_DATE']  # yyyy-mm-dd
job_run_id = args['JOB_RUN_ID'] if 'JOB_RUN_ID' in args else datetime.datetime.utcnow().strftime('%Y%m%d%H%M%S')
s3_output_versioned = f"{args['S3_OUTPUT_PATH'].rstrip('/')}/{run_date}/{job_run_id}/"
s3_log_versioned = f"{args['S3_LOG_PATH'].rstrip('/')}/{run_date}/{job_run_id}/"


# ==============================
# Inicialización de contexto Glue
# ==============================
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# ==============================
# Lectura de datos
# ==============================
df = spark.read.csv(args['S3_INPUT_PATH'], header=True, inferSchema=True)

# ==============================
# Filtrado de mensajes de usuario
# ==============================
df_user = df.filter(col('inbound') == True)

# ==============================
# Limpieza y normalización de texto
# ==============================
@udf(StringType())
def clean_text(text):
    """
    Limpia el texto eliminando caracteres especiales y convirtiendo a minúsculas.
    """
    if text is None:
        return ""
    text = text.lower()
    text = re.sub(r"http\S+", "", text)
    text = re.sub(r"[^a-zA-Z0-9\s]", "", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()

df_user = df_user.withColumn("clean_text", clean_text(col("text")))

# ==============================
# Feature engineering
# ==============================
@udf(IntegerType())
def count_token(text, token):
    """
    Cuenta la cantidad de veces que aparece un token específico en el texto.
    """
    if text is None:
        return 0
    return text.count(token)

df_user = df_user.withColumn("num_question_marks", count_token(col("clean_text"), lit("?")))
df_user = df_user.withColumn("num_exclamations", count_token(col("clean_text"), lit("!")))

# Palabras clave (puedes ajustar la lista)
critical_words = ['problem', 'issue', 'not working', 'refund', 'error', 'fail', 'help', 'wtf', 'worst', 'urgent', 'bad']
for word in critical_words:
    @udf(IntegerType())
    def has_word(text, w=word):
        """
        Verifica si una palabra clave está presente en el texto.
        """
        if text is None:
            return 0
        return int(w in text)
    df_user = df_user.withColumn(f"has_{word.replace(' ', '_')}", has_word(col("clean_text")))

# ==============================
# Guardado de resultado versionado
# ==============================
output_cols = ['clean_text', 'num_question_marks', 'num_exclamations'] + [f"has_{word.replace(' ', '_')}" for word in critical_words]

df_user.select(output_cols).write.mode('overwrite').parquet(s3_output_versioned)

# ==============================
# Logging: cantidad de registros, stats principales
# ==============================
log_text = f"""
Preprocessing job run: {job_run_id}
Run date: {run_date}
Source S3: {args['S3_INPUT_PATH']}
Output S3: {s3_output_versioned}
Total input records: {df.count()}
Total user messages (inbound): {df_user.count()}
"""

# Guarda log como txt en S3
log_df = spark.createDataFrame([(log_text,)], ["log"])
log_df.write.mode('overwrite').text(s3_log_versioned)

print("Preprocessing finished and logs saved.")

