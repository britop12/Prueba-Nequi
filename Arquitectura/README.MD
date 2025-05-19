
# Arquitectura Técnica Propuesta para Clasificación Batch de Mensajes de Usuario
![Arquitectura](Arquitectura/Arquitectura_NLP.jpg)

## **1. Ingesta y Gobierno de Datos**

* **Amazon S3** centraliza el almacenamiento de los datos crudos provenientes de distintas fuentes.
* Los logs de procesamiento y ejecución se almacenan en **S3** y **CloudWatch Logs** para auditoría y trazabilidad.

## **2. Preprocesamiento y ETL**

* **AWS Glue** ejecuta trabajos de preprocesamiento y limpieza de datos en modalidad batch, orquestados a través de  **AWS Step Functions**.
* Todas las transformaciones, validaciones y reportes de calidad de datos quedan registradas en **S3** y **CloudWatch Logs**.

## **3. Desarrollo Colaborativo y Versionado**

* **Amazon SageMaker Studio** es el entorno principal para experimentación, prototipado y entrenamiento de modelos, permitiendo colaboración entre equipos.
* **AWS CodeCommit** y **ECR** permiten el versionado de código fuente y ambientes.
* Los artefactos de modelos son almacenados y versionados en **S3**.

## **4. Orquestación y Automatización**

* **AWS Step Functions** define y ejecuta el pipeline batch, integrando los pasos de preprocesamiento, entrenamiento y despliegue.
* **EventBridge** programa ejecuciones automáticas (por tiempo o por eventos de negocio).

## **5. Entrenamiento y Registro de Modelos**

* El entrenamiento del modelo se ejecuta de forma orquestada y controlada dentro del pipeline.
* Los modelos entrenados se almacenan en  **SageMaker Model Registry**, donde un **ML Engineer** revisa y aprueba el modelo antes del despliegue en producción.

## **6. Despliegue e Inferencia Batch**

* **SageMaker Batch Transform** realiza la inferencia masiva sobre nuevos datos, con capacidad de auto-escalado según la carga de trabajo.
* Los resultados de la inferencia se almacenan nuevamente en **S3**.

## **7. Monitoreo**

* **SageMaker Model Monitor** supervisa la calidad del modelo en producción, detectando automáticamente data drift, model drift y outliers en las predicciones.
* **CloudWatch Metrics** recolecta y centraliza las métricas generadas por **SageMaker Model Monitor**.
* **CloudWatch Alarm** detecta anomalías en las métricas e integra con **SNS** para enviar notificaciones automáticas (correo/SMS) a los administradores responsables.
* Todos los logs relevantes se almacenan en **S3** y **CloudWatch Logs** para fines de auditoría.

## **8. Seguridad** 

* El acceso a cada recurso está controlado mediante políticas de **IAM** siguiendo el principio de mínimo privilegio.
* Los recursos críticos (S3, Glue, SageMaker, Model Registry) cuentan con cifrado en reposo y en tránsito.
* Los permisos y funciones de cada usuario (Data Scientist, Data Engineer, ML Engineer, Admin) estarán definidos.
* El sistema facilita la auditoría de todas las acciones relevantes, cumpliendo con estándares regulatorios y de gobernanza de datos.

## 9. Escalabilidad y Gobierno Avanzado de Datos

A medida que el sistema crezca o los requisitos regulatorios y de trazabilidad aumenten, esta arquitectura permite incorporar fácilmente servicios de gobernanza y gestión avanzada de datos de AWS:

* **AWS Lake Formation:** Para definir y aplicar políticas centralizadas de acceso, gobierno y seguridad sobre los datos tabulares almacenados en S3.
* **AWS Glue Catalog:** Para mantener un catálogo centralizado de metadatos de datasets, facilitando el descubrimiento, versionado y control de esquemas.
* **SageMaker Feature Store:** Para gestionar, versionar y reutilizar features (variables) en diferentes modelos y pipelines, garantizando trazabilidad y consistencia entre entrenamiento e inferencia.

Estos componentes pueden integrarse de forma modular cuando el volumen de datos, la colaboración entre equipos, o los requisitos regulatorios lo demanden, sin afectar la robustez ni la mantenibilidad del pipeline actual.
