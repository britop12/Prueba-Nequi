# Entrenamiento e Inferencia del Modelo

El entrenamiento comienza cargando datos previamente procesados en formato Parquet generados en la etapa previa (AWS Glue). Estos datos contienen columnas derivadas del preprocesamiento, incluyendo texto limpio y características adicionales relacionadas con palabras clave críticas (p.ej.: `has_problem`, `has_refund`).

## Etiquetado automático del dataset

Para entrenar un modelo supervisado, generamos automáticamente una etiqueta binaria para cada mensaje usando una función lógica definida:

* Mensajes con al menos una palabra crítica (por ejemplo: "problem", "refund", "error") se etiquetan como  **"critical"** .
* El resto se etiquetan como  **"non_critical"** .

Esta metodología simple pero efectiva y rápida, genera un dataset claro para la tarea de clasificación binaria, facilitando el entrenamiento.

## División del dataset (Train-Test split)

Es necesario mencionar, antes de hacer el split del dataset, se tiene como única variable `clean_text`, porque de esta variable se hace la extracción de características.

El dataset etiquetado se divide en conjuntos de entrenamiento y prueba, garantizando reproducibilidad mediante `random_state`. Y finalmente, se seleccionó una división del 70%-30% para garantizar suficiente cantidad de datos de prueba y obtener una evaluación confiable.

## Entrenamiento del modelo

El modelo utiliza una estrategia simple pero robusta, empleando un pipeline de `sklearn` que incluye:

* **TfidfVectorizer** : Convierte texto a vectores numéricos usando TF-IDF con un máximo de 500 características (palabras más importantes).
* **Logistic Regression** : Clasificador sencillo pero potente, equilibrado por peso de clases para gestionar desbalances.

## Evaluación y reporte de métricas

La calidad del modelo entrenado se evalúa sobre el conjunto de prueba usando métricas estándar, generando un reporte de clasificación detallado (precisión, recall, F1-score). Adicionalmente, métricas clave se extraen específicamente para análisis rápido y seguimiento:

* **F1-score (clase crítica)** : Mide la efectividad del modelo en detectar mensajes críticos.
* **Accuracy** : Proporción de clasificaciones correctas totales.
* **Macro-F1** : Promedio balanceado del rendimiento sobre ambas clases.

Estas métricas son almacenadas en un diccionario estructurado, que para futuro se usarán para ver si se aprueba o no el modelo.

## Almacenamiento y versionado en Amazon S3

El modelo entrenado y sus métricas también se guardan en un bucket centralizado de Amazon S3. La estructura utilizada facilita rastrear cada versión generada del modelo y permite integración automática con otros componentes del sistema, como SageMaker Model Registry:

* Modelo: `models/nequi-risk-nlp/model-<fecha-hora>/model.joblib`
* Métricas: `models/nequi-risk-nlp/model-<fecha-hora>/metrics.json`

## Pipeline de inferencia Batch

La etapa de inferencia utiliza el modelo almacenado en S3 mediante el servicio SageMaker Batch Transform. Este pipeline automatiza completamente el proceso:

1. Se ejecuta nuevamente el pipeline de Glue para preprocesar nuevos datos (asegurando consistencia con la etapa de entrenamiento).
2. Una función Lambda obtiene automáticamente el modelo aprobado más reciente desde SageMaker Model Registry.
3. SageMaker Batch Transform realiza inferencia sobre los nuevos datos.
4. Los resultados se guardan automáticamente en S3 para posterior análisis o consumo por otras aplicaciones.

Este proceso es ejecutado automáticamente mediante AWS Step Functions, asegurando reproducibilidad, escalabilidad y control operativo robusto.
