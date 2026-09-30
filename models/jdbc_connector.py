def guardar_en_sql(df, nombre_tabla, modo="overwrite"):
    url = "jdbc:mysql://localhost:3306/IBEX35"
    
    propiedades = {
        "driver": "com.mysql.cj.jdbc.Driver",
        "user": "root",  
        "password": "TuPasswordFuerte123!"
    }
    
    df.write.jdbc(url=url, table=nombre_tabla, mode=modo, properties=propiedades)