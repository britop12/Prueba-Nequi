## Estrategia de Monitoreo

El monitoreo en el pipeline se centra en asegurar:

* **Disponibilidad operativa:** garantizar que todos los componentes clave del pipeline (Glue Jobs, SageMaker Training Jobs, Lambda Functions, Transform Jobs) estén funcionando continuamente.
* **Calidad de datos y modelos:** detectar automáticamente cambios o desviaciones significativas (drift) en la calidad de los datos y en el rendimiento del modelo.
* **Acciones proactivas:** automatizar respuestas inmediatas (notificaciones y reentrenamientos) en función del monitoreo constante.

### 1. Monitoreo del Preprocesamiento y Entrenamiento

Durante la etapa de preprocesamiento (AWS Glue) y entrenamiento (SageMaker):

* Se activan automáticamente **CloudWatch Logs** para registrar eventos detallados, tiempos de ejecución, errores y advertencias, permitiendo auditorías completas.
* Métricas clave como tiempos de ejecución, uso de recursos (CPU, memoria) y estado del job (exitoso o fallido) se recolectan automáticamente.
* Errores críticos se notifican mediante alertas automáticas usando  **AWS SNS** , enviando información específica del error al equipo responsable.

### 2. Monitoreo del Modelo y Desempeño (Model Monitor & CloudWatch)

Una vez que el modelo se despliega en producción, se utiliza **Amazon SageMaker Model Monitor** para evaluar continuamente las predicciones generadas por el modelo:

* Se establece un baseline inicial después del primer despliegue del modelo.
* Posteriormente, Model Monitor evalúa métricas como **data drift** (cambio en la distribución de datos) y **model drift** (disminución en rendimiento del modelo, por ejemplo, una caída del F1-score por debajo del umbral establecido).
* Cuando se detecta un drift o anomalía significativa, **CloudWatch Alarm** genera una alerta inmediata, que se comunica al administrador del sistema vía SNS y puede disparar automáticamente la ejecución del pipeline de entrenamiento mediante  **EventBridge** , iniciando así el reentrenamiento.

## Estrategia de Seguridad

La seguridad del pipeline propuesto se asegura mediante una serie de controles aplicados a diferentes niveles:

### 1. Gestión de Accesos y Permisos (IAM)

* **AWS Identity and Access Management (IAM)** se utiliza con políticas estrictas bajo el principio de "mínimo privilegio" (least privilege), asegurando que cada usuario (Data Scientist, Data Engineer, ML Engineer, Admin) tenga acceso únicamente a los recursos estrictamente necesarios para cumplir sus tareas.
* Cada recurso (Glue, SageMaker, S3, Lambda, Step Functions, Model Registry) tendrá asignados roles IAM específicos, con permisos explícitos y auditables.

### 2. Seguridad del Modelo y Versionado (SageMaker Model Registry)

* Los modelos entrenados se almacenan y gestionan centralmente en  **SageMaker Model Registry** , que proporciona mecanismos integrados para asegurar la gobernanza, la gestión de versiones, aprobación manual o automática (mediante métricas validadas) y auditoría completa de cambios y aprobaciones. Por ejemplo, para desplegar en producción se puede usar Human-in-the-loop, para que apruebe el despliegue de un modelo.
* Solo modelos explícitamente marcados como "Approved" pueden desplegarse en producción.
