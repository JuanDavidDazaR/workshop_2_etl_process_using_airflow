# taller_2_proceso_etl_usando_airflow

Canalización ETL que utiliza Apache Airflow para extraer, transformar y cargar datos de múltiples fuentes (API, CSV y base de datos). Los datos procesados ​​se almacenan en una base de datos y en Google Drive, y se visualizan mediante un panel de control.

---

## 🚀 Pasos de instalación (WSL2)

### 1. Instalar PostgreSQL (última versión)

```bash
sudo apt update
sudo apt install wget ca-certificates -y
wget -qO - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -
echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" | sudo tee /etc/apt/sources.list.d/pgdg.list
sudo apt update
sudo apt install postgresql -y
```

Inicia el servicio:

```bash
sudo service postgresql start
```

Crea el usuario y las bases de datos:

```bash
sudo -u postgres psql
```

Dentro de `psql`:

```sql
CREATE USER postgres WITH PASSWORD 'pg';
ALTER USER postgres CREATEDB;
\q
```


### 2. Crear entorno virtual (fuera de la carpeta del repositorio)

```bash
python3 -m venv venv
source venv/bin/activate
```

---

### 3. Instalar Apache Airflow

```bash
pip install apache-airflow
```

### 5. Crear archivo `.env` en el directorio clonado

Dentro de  de la caperta edl proyecto crea el archivo `.env`:

```env
DB_HOST=localhost
DB_PORT=5432
DB_NAME= "nombre que quieras para la abse de datos del grammys"
DB_NAME_LOAD = "nombre de la base que quieras aquis e sube el merge"
DB_USER=postgres
DB_PASSWORD="tu contraseña de postgres"
```

Para obtener la IP de tu máquina WSL2:

```bash
hostname -I
```

---

### 6. Instalar requerimientos del proyecto

Desde el entorno virtual activo:

```bash
pip install -r requirements.txt
```

---

### 7. Ejecutar los notebooks (antes de iniciar Airflow)

Los notebooks cargan y transforman los datos necesarios en las bases de datos. Es **obligatorio ejecutarlos antes de iniciar Airflow**.

```bash
jupyter lab
```

Corre los siguientes notebooks, en orden, como esatn enumerados

---

### 8. Inicializar Airflow

```bash
airflow standalone
```

Esto creará la carpeta `~/airflow` y levantará el servidor web.

Abre [http://localhost:8080](http://localhost:8080) y usa las credenciales generadas en la terminal para acceder.

---
---
importante que entres al archivo de airflow y dirigas los dgas al siguiente ruta
```
workshop_2_etl_process_using_airflow/dags
```

---
---

## 📁 Notebooks

Puedes encontrar los notebooks en la carpeta `notebooks/` del repositorio clonado. Asegúrate de ejecutarlos en el orden correcto.

---

## ✅ Ejecutar los DAGs

Desde el panel web de Airflow:

- Habilita y ejecuta los DAGs disponibles.
- Asegúrate de que el entorno virtual esté activo si haces pruebas en consola.

---

## 📊 Integración con Power BI

1. Abre Power BI Desktop.
2. Selecciona _Obtener datos → PostgreSQL_.
3. Conecta usando:
   - **Servidor**: IP obtenida con `hostname -I`
   - **Base de datos**:  o `spotify_grammy_merged`
   - **Autenticación**: `postgres / pg`




## 📌 Requisitos

- Python 3.12+
- PostgreSQL (última versión disponible en WSL2)
- Apache Airflow
- Power BI Desktop
- Visual Studio Code o editor de tu elección

---