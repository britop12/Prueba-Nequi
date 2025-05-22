# Arquitectura Técnica Propuesta para Clasificación Batch de Mensajes de Usuario

![Arquitectura](https://github.com/britop12/Prueba-Nequi/blob/main/Arquitectura/Arquitectura_NLP.jpg?raw=true)

## 1. Ingesta y Gobierno de Datos

El proceso comienza con la ingesta y almacenamiento inicial de los datos crudos provenientes de diversas fuentes externas. Estos datos son centralizados en Amazon S3, permitiendo un almacenamiento altamente escalable, seguro y con versionado automático. Adicionalmente, se almacenan registros y logs detallados de cada ejecución tanto en S3 como en Amazon CloudWatch Logs, garantizando completa auditoría y trazabilidad durante todo el proceso.

## 2. Preprocesamiento y Transformaciones (ETL)

La fase de preprocesamiento está gestionada mediante AWS Glue, que ejecuta scripts de limpieza, validación y transformación de los datos en modalidad batch. Esta ejecución es orquestada mediante AWS Step Functions, asegurando consistencia, reproducibilidad y automatización completa. Los resultados de estas transformaciones, junto con validaciones y reportes automáticos de calidad de datos, son almacenados en S3 y CloudWatch Logs, facilitando auditorías posteriores y depuraciones.

## 3. Desarrollo Colaborativo, Versionado y Experimentación

Amazon SageMaker Studio funciona como entorno principal para el desarrollo colaborativo, experimentación y entrenamiento de modelos. Los científicos de datos pueden explorar rápidamente nuevas hipótesis y versiones del modelo en entornos controlados.

AWS CodeCommit y Amazon ECR complementan esta etapa proporcionando un control robusto del versionado tanto del código fuente de los modelos como de los entornos utilizados (contenedores Docker). Además, todos los artefactos de modelo generados durante esta etapa (modelos entrenados, métricas y scripts) se almacenan y versionan centralmente en Amazon S3, asegurando reproducibilidad total de cualquier ejecución.

## 4. Orquestación Automática del Pipeline

Para garantizar una ejecución robusta, confiable y totalmente automatizada del proceso de ML, AWS Step Functions coordina y orquesta todo el pipeline batch: desde el preprocesamiento hasta el entrenamiento y registro del modelo final.

AWS EventBridge complementa esta automatización permitiendo la ejecución programada (semanalmente o en intervalos definidos por el negocio) del pipeline de entrenamiento, asegurando un reentrenamiento automático constante. Adicionalmente, EventBridge podría disparar ejecuciones basadas en eventos específicos del sistema, como la detección de degradación en métricas del modelo (drift).

Por otro lado, si el negocio lo requiere, mediante Trigger con Lambda, se puede disparar la ejecución del pipeline de entrenamiento, apenas se suban nuevos datos de entrenamiento al Bucket de S3.

## 5. Entrenamiento y Gestión de Modelos

La etapa de entrenamiento del modelo se realiza de forma automatizada dentro del pipeline. Una vez finalizado el entrenamiento, el modelo resultante es evaluado automáticamente en cuanto a métricas clave (por ejemplo, F1-score). Si cumple con el umbral definido, es registrado en SageMaker Model Registry, proporcionando una gobernanza centralizada del modelo.

El ML Engineer tiene entonces la capacidad de aprobar manual o automáticamente (en función de métricas validadas) cada modelo antes de su despliegue en producción, garantizando la calidad y control completo sobre el estado del modelo desplegado.

## 6. Inferencia Automática en Producción

La inferencia en producción es gestionada mediante SageMaker Batch Transform, ejecutando predicciones masivas sobre nuevos datos entrantes. Este servicio tiene capacidad de autoescalado, respondiendo eficientemente a picos de carga.

Los resultados generados son almacenados nuevamente en Amazon S3, garantizando consistencia, versionado determinístico (gracias a variables como `run_date` y `run_id`) y fácil integración con otras herramientas analíticas del negocio.

## 7. Monitoreo Continuo y Acciones Correctivas

El sistema incorpora un monitoreo constante mediante SageMaker Model Monitor, que permite la detección temprana y automática de anomalías como data drift, model drift o la presencia de outliers en predicciones generadas. Estas métricas clave son centralizadas mediante CloudWatch Metrics, facilitando la visualización y generación automática de alarmas mediante CloudWatch Alarm.

Ante la detección de problemas o desviaciones significativas, CloudWatch Alarm notifica inmediatamente a través de AWS SNS a los administradores y equipos operativos responsables, permitiendo intervenciones rápidas y automáticas, como el reentrenamiento del modelo.

## 8. Seguridad Integral del Sistema

La seguridad del sistema se gestiona mediante la aplicación estricta del principio de mínimo privilegio utilizando AWS Identity and Access Management (IAM). Cada usuario del sistema (Data Scientist, Data Engineer, ML Engineer y Admin) posee roles y políticas específicas, garantizando accesos apropiados y seguros.

## 9. Escalabilidad y Gobierno Avanzado de Datos

La arquitectura propuesta está diseñada con visión futura, permitiendo una fácil escalabilidad cuando el volumen de datos o requerimientos regulatorios incrementen significativamente. Servicios avanzados como AWS Lake Formation, AWS Glue Catalog y SageMaker Feature Store pueden integrarse de forma modular y sencilla:

* Lake Formation podría facilitar la gobernanza centralizada de datos, aplicando políticas avanzadas de acceso y seguridad sobre grandes volúmenes de datos almacenados en S3.
* Glue Catalog permitiría mantener un catálogo organizado y versionado de metadatos y esquemas utilizados.
* Feature Store ayudaría en el manejo eficiente y reutilización consistente de características (variables predictivas), garantizando la trazabilidad completa entre etapas de entrenamiento e inferencia.

## Conclusión Técnica de la Propuesta

Esta arquitectura cubre rigurosamente todas las etapas del ciclo de vida de un modelo ML de clasificación automática de mensajes en batch: desde la ingesta, procesamiento, entrenamiento, inferencia, hasta el monitoreo y seguridad continuos. Cada componente ha sido seleccionado y diseñado para reflejar mejores prácticas de ingeniería MLOps, proporcionando modularidad, reproducibilidad, escalabilidad y gobernanza robusta.
