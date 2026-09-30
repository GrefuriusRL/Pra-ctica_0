# Pra-ctica_0
Dentro de la carpeta src encontramos que el proyecto esta organizado de cierta manera que se utilizan rutas realtivas para poder ejecutarse en cualquier dispositivo.
El proyecto esta divido en cuatro carpetas principlaes:

--config:
    En esta carpeta se encuentra el archivo spark_session.py que consta de la funcion get_spark_session() utilizada para poder utilizar spark 

--data:
    Aqui podemos encontrar el csv con los datos utilizados

--drivers:
    Este directorio contiene el connector jdbc de mysql (mysql-connector-j-26.7.0.jar)

--models:
    Esta carpeta aloja el archivo jdbc_connector.py que contiene la función guardar_en_sql() la cual permite conectarse con la base de datos y escribir los nuevos datos deseados

Ademas de estas cuatro carpetas se encontrará el archivo *main.py* el cual actua como controlador principal, al ejecutarse mostrara en consola la solución de los ejercicios propuestos 

Conexion a base de datos:
En mi caso particular para poder usar una base de datos y conectarme a ella opte por utilizar un docker que ya tenía creada por lo que antes de poder guardar los datos debia utilizar el comando sudo docker start mysql-paladex y utilizar mis credenciales:          
        "driver": "com.mysql.cj.jdbc.Driver",
        "user": "root",  
        "password": "TuPasswordFuerte123!"
las cuales se pueden modificar para conectarse a otra base de datos cambiandolos en el archibo jdbc_connector.py 


Para poder ejecutar este codigo se requieren unas ciertas librerias para lo cual recomiendo el metodo que yo utilize que es crear un entrorno con conda:

conda create --name mineriadedatos2026 python=3.10
conda activate mineriadedatos2026
conda install -c conda-forge pyspark=3.3
conda install -c conda-forge openjdk=17
conda install -c anaconda openpyxl=3.1.5 graphviz

siguiendo estos comandos si se abre una terminal y se viaja a la carpeta src se puede ejecutar el proyecto con estos 2 simples comandos:

conda activate mineriadedatos2026
python main.py
