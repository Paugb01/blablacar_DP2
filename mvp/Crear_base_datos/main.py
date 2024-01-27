import psycopg2


# 1- Conectar a la base de datos PostgreSQL
conn = psycopg2.connect(
    host="postgres", #SI ES PARA EL DOCKER-COMPOSE, CAMBIAR POR "postgres", en local igual a "localhost"
    database="postgres",
    user="postgres",
    password="postgres",
    port="5432"
)


# Crear un cursor para ejecutar comandos SQL
cursor = conn.cursor()

# 2- generar tabla viajes, conductores y pasajeros

cursor.execute("""
    DROP TABLE IF EXISTS public.viajes;
    
    CREATE TABLE IF NOT EXISTS public.viajes (
        viaje_id SERIAL PRIMARY KEY,
        conductor_id INTEGER,
        pasajero_id INTEGER,
        numero_pasajeros INTEGER,
        precio FLOAT
    );
""")

conn.commit()


cursor.execute("""
    DROP TABLE IF EXISTS public.conductores;
    
    CREATE TABLE IF NOT EXISTS public.conductores (
        conductor_id SERIAL PRIMARY KEY,
        matricula VARCHAR(100)
    );
""")

conn.commit()


cursor.execute("""
    DROP TABLE IF EXISTS public.pasajeros;
    
    CREATE TABLE IF NOT EXISTS public.pasajeros (
        pasajero_id SERIAL PRIMARY KEY
    );
""")

conn.commit()

# 3- configurar las foreign keys

cursor.execute("""
    ALTER TABLE public.viajes ADD CONSTRAINT viajes_conductores
FOREIGN KEY (conductor_id) REFERENCES public.conductores(conductor_id);

ALTER TABLE public.viajes ADD CONSTRAINT viajes_pasajeros 
FOREIGN KEY (pasajero_id) REFERENCES public.pasajeros(pasajero_id);


""")

conn.commit()


# 4- Cerrar la conexión
cursor.close()
conn.close()

print('DONE')