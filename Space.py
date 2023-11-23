import os
import psycopg2
import random
import time
from faker import Faker
from psycopg2 import sql

fake = Faker()
limpiar = os.system("cls")

print("""


                ███    ███ ██     ██████   █████  ████████  █████  ██████   █████  ███████ ███████     
                ████  ████ ██     ██   ██ ██   ██    ██    ██   ██ ██   ██ ██   ██ ██      ██          
                ██ ████ ██ ██     ██   ██ ███████    ██    ███████ ██████  ███████ ███████ █████       
                ██  ██  ██ ██     ██   ██ ██   ██    ██    ██   ██ ██   ██ ██   ██      ██ ██          
                ██      ██ ██     ██████  ██   ██    ██    ██   ██ ██████  ██   ██ ███████ ███████     
                                                                                                       
                                                                                                    

""")

# Funcion para conetarse a nuestra base de datos
def conectar():
    return psycopg2.connect(
        dbname="Marte",
        user="postgres",
        password="supreme",
        host="localhost",
        port="5432"
    )

# Funciones para poblar cada una de las tablas 
def poblar_trabajador(conn, num_records):
    try:
        # Abre un cursor para ejecutar consultas SQL
        cur = conn.cursor()

        # Especifica el esquema para las inserciones
        schema_name = "millon_datos"

        # Inserta registros aleatorios en la tabla Trabajador
        num = 10000000
        for _ in range(num_records):
            image = fake.image_url()
            fecha_nacimiento = fake.date_of_birth(minimum_age=25, maximum_age=70)
            genero = fake.random_element(elements=('masculino', 'femenino'))
            dni = num
            fecha_contratacion = fake.date_between(start_date='-365d', end_date='today')
            salario = fake.random_int(min=30000, max=90000)
            first_name = fake.first_name()
            last_name = fake.last_name()
            num += 1

            # Imprime los datos antes de la inserción
            #print(f"Insertando: {image}, {fecha_nacimiento}, {genero}, {dni}, {fecha_contratacion}, {salario}, {first_name}, {last_name}")

            # Ejecuta la inserción
            cur.execute(sql.SQL("""
                INSERT INTO {}.Trabajador (
                    image,
                    fechaNacimiento,
                    genero,
                    dni,
                    fechaContratacion,
                    salario,
                    firstName,
                    lastName
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s
                )
            """).format(sql.Identifier(schema_name)), (
                image,
                fecha_nacimiento,
                genero,
                dni,
                fecha_contratacion,
                salario,
                first_name,
                last_name
            ))

        # Confirma la transacción
        conn.commit()

        print("     1.Conexión exitosa y las inserciones en la tabla Trabajador se realizaron correctamente.")

    except Exception as e:
        print(f"Error: {e}")

def poblar_planeta(conn, num_records):
    try:
        # Abre un cursor para ejecutar consultas SQL
        cur = conn.cursor()

        # Especifica el esquema para las inserciones
        schema_name = "millon_datos"

        # Inserta registros aleatorios en la tabla Planeta
        for _ in range(num_records):
            gravedad_planeta = round(fake.random_number(digits=3, fix_len=True) * 0.001, 3)
            distancia = round(fake.random_number(digits=5, fix_len=True) * 0.01, 2)
            habitado = fake.random_element(elements=(True, False))
            explorado = fake.random_element(elements=(True, False))

            # Imprime los datos antes de la inserción
            #print(f"Insertando: {gravedad_planeta}, {distancia}, {habitado}, {explorado}")

            # Ejecuta la inserción
            cur.execute(sql.SQL("""
                INSERT INTO {}.Planeta (
                    gravedadPlaneta,
                    distancia,
                    habitado,
                    explorado
                ) VALUES (
                    %s, %s, %s, %s
                )
            """).format(sql.Identifier(schema_name)), (
                gravedad_planeta,
                distancia,
                habitado,
                explorado
            ))

        # Confirma la transacción
        conn.commit()

        print("     2.Conexión exitosa y las inserciones en la tabla Planeta se realizaron correctamente.")

    except Exception as e:
        print(f"Error: {e}")

def poblar_mision(conn, num_records):
    try:
        # Abre un cursor para ejecutar consultas SQL
        cur = conn.cursor()

        # Especifica el esquema para las inserciones
        schema_name = "millon_datos"

        # Obtén la lista de identificadores de planetas existentes
        cur.execute(sql.SQL("SELECT identificarPlaneta FROM {}.Planeta").format(sql.Identifier(schema_name)))
        planet_ids = [row[0] for row in cur.fetchall()]


        # Inserta registros aleatorios en la tabla Mision
        for _ in range(num_records):
            id_planeta = fake.random_element(elements=planet_ids)
            nombre_mision = fake.word()
            cantidad_presupuesto = fake.random_int(min=100000, max=1000000)
            inicio_mision = fake.date_between(start_date='-365d', end_date='today')
            fin_mision = fake.date_between(start_date='today', end_date='+365d')
            
            # Genera un objetivo de misión con una longitud máxima de 50 caracteres
            objetivo_mision = fake.text(max_nb_chars=50)

            # Imprime los datos antes de la inserción
            #print(f"Insertando: {id_planeta}, {nombre_mision}, {cantidad_presupuesto}, {inicio_mision}, {fin_mision}, {objetivo_mision}")

            # Ejecuta la inserción
            cur.execute(sql.SQL("""
                INSERT INTO {}.Mision (
                    Id_Planeta,
                    nombreMision,
                    cantidadPresupuesto,
                    inicioMision,
                    finMision,
                    objetivoMision
                ) VALUES (
                    %s, %s, %s, %s, %s, %s
                )
            """).format(sql.Identifier(schema_name)), (
                id_planeta,
                nombre_mision,
                cantidad_presupuesto,
                inicio_mision,
                fin_mision,
                objetivo_mision
            ))

        # Confirma la transacción
        conn.commit()

        print("     3.Conexión exitosa y las inserciones en la tabla Mision se realizaron correctamente.")

    except Exception as e:
        print(f"Error: {e}")

def poblar_cohete(conn, num_records):
    try:
        # Abre un cursor para ejecutar consultas SQL
        cur = conn.cursor()

        # Especifica el esquema para las inserciones
        schema_name = "millon_datos"

        # Obtén la lista de identificadores de misiones existentes
        cur.execute(sql.SQL("SELECT identificadorMision FROM {}.Mision").format(sql.Identifier(schema_name)))
        mision_ids = [row[0] for row in cur.fetchall()]

        # Inserta registros aleatorios en la tabla Cohete
        for _ in range(num_records):
            id_mision = fake.random_element(elements=mision_ids)
            nombre_cohete = fake.word()
            longitud_cohete = round(random.uniform(10, 100), 2)
            empuje = round(random.uniform(100000, 500000), 2)
            peso_despegue = round(random.uniform(50000, 200000), 2)
            diametro = round(random.uniform(1, 10), 2)
            velocidad_maxima = round(random.uniform(5000, 20000), 2)
            cantidad_etapa = fake.random_int(min=1, max=10)
            carga_util = fake.random_int(min=1000, max=10000)

            # Imprime los datos antes de la inserción
            #print(f"Insertando: {id_mision}, {nombre_cohete}, {longitud_cohete}, {empuje}, {peso_despegue}, {diametro}, {velocidad_maxima}, {cantidad_etapa}, {carga_util}")

            # Ejecuta la inserción
            cur.execute(sql.SQL("""
                INSERT INTO {}.Cohete (
                    idMision,
                    nombreCohete,
                    longitudCohete,
                    empuje,
                    pesoDespegue,
                    diametro,
                    velocidadMaxima,
                    cantidadEtapa,
                    cargaUtil
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
            """).format(sql.Identifier(schema_name)), (
                id_mision,
                nombre_cohete,
                longitud_cohete,
                empuje,
                peso_despegue,
                diametro,
                velocidad_maxima,
                cantidad_etapa,
                carga_util
            ))

        # Confirma la transacción
        conn.commit()

        print("     4.Conexión exitosa y las inserciones en la tabla Cohete se realizaron correctamente.")

    except Exception as e:
        print(f"Error: {e}")

def poblar_lanzamiento(conn, num_records):
    try:
        # Abre un cursor para ejecutar consultas SQL
        cur = conn.cursor()

        # Especifica el esquema para las inserciones
        schema_name = "millon_datos"

        # Obtén la lista de identificadores de misiones existentes
        cur.execute(sql.SQL("SELECT identificadorMision FROM {}.Mision").format(sql.Identifier(schema_name)))
        mision_ids = [row[0] for row in cur.fetchall()]

        # Inserta registros aleatorios en la tabla Lanzamiento
        for _ in range(num_records):
            id_mision = fake.random_element(elements=mision_ids)
            fecha_lanzamiento = fake.date_between(start_date='-365d', end_date='today')
            lugar_lanzamiento = fake.word()

            # Imprime los datos antes de la inserción
            #print(f"Insertando: {id_mision}, {fecha_lanzamiento}, {lugar_lanzamiento}")

            # Ejecuta la inserción
            cur.execute(sql.SQL("""
                INSERT INTO {}.Lanzamiento (
                    IdMision,
                    fechaLanzamiento,
                    lugarLanzamiento
                ) VALUES (
                    %s, %s, %s
                )
            """).format(sql.Identifier(schema_name)), (
                id_mision,
                fecha_lanzamiento,
                lugar_lanzamiento
            ))

        # Confirma la transacción
        conn.commit()

        print("     5.Conexión exitosa y las inserciones en la tabla Lanzamiento se realizaron correctamente.")

    except Exception as e:
        print(f"Error: {e}")

def poblar_directorvuelo(conn, num_records):
    def generar_valor_aleatorio():
        return fake.random_int(min=0, max=100)

    try:
        # Conéctate a tu base de datos local
        conn = psycopg2.connect(
            dbname="Marte",
            user="postgres",
            password="supreme",
            host="localhost",
            port="5432"
        )

        # Abre un cursor para ejecutar consultas SQL
        cur = conn.cursor()

        # Especifica el esquema para las inserciones
        schema_name = "millon_datos"

        # Obtén la lista de identificadores de trabajadores existentes
        cur.execute(sql.SQL("SELECT identificadorTrabajor FROM {}.Trabajador").format(sql.Identifier(schema_name)))
        trabajador_ids = [row[0] for row in cur.fetchall()]

        # Inserta registros aleatorios en la tabla DirectorVuelo
        for _ in range(num_records):
            viaje_iss = generar_valor_aleatorio()
            viaje_interplanetario = generar_valor_aleatorio()

            # Asegúrate de que haya identificadores de trabajadores disponibles
            if not trabajador_ids:
                break

            # Selecciona un identificador de trabajador existente
            identificador_trabajador = fake.random_element(elements=trabajador_ids)

            # Elimina el identificador seleccionado para evitar duplicados
            trabajador_ids.remove(identificador_trabajador)

            # Selecciona atributos específicos del trabajador
            cur.execute("""
                SELECT image, fechaNacimiento, genero, dni, fechaContratacion, salario, firstName, lastName
                FROM {}.Trabajador
                WHERE identificadorTrabajor = %s
            """.format(schema_name), (identificador_trabajador,))

            # Obtiene los atributos del trabajador
            atributos_trabajador = cur.fetchone()

            # Imprime los datos antes de la inserción en DirectorVuelo
            #print(f"Insertando en DirectorVuelo: {viaje_iss}, {viaje_interplanetario}")

            # Ejecuta la inserción en DirectorVuelo
            cur.execute("""
                INSERT INTO {}.DirectorVuelo (
                    ViajeISS,
                    ViajeInterplanetario,
                    identificadorTrabajor,
                    image,
                    fechaNacimiento,
                    genero,
                    dni,
                    fechaContratacion,
                    salario,
                    firstName,
                    lastName
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
            """.format(schema_name), (
                viaje_iss,
                viaje_interplanetario,
                identificador_trabajador,
                *atributos_trabajador
            ))

        # Confirma la transacción
        conn.commit()

        print("     6.Conexión exitosa y las inserciones en la tabla DirectorVuelo se realizaron correctamente.")
    except Exception as e:
        print(f"Error: {e}")

def poblar_ingeniero_vuelo(conn, num_records):
    def generar_valor_aleatorio():
        return fake.random_int(min=0, max=100)

    try:
        # Abre un cursor para ejecutar consultas SQL
        cur = conn.cursor()

        # Especifica el esquema para las inserciones
        schema_name = "millon_datos"

        # Obtén la lista de identificadores de trabajadores existentes en DirectorVuelo
        cur.execute(sql.SQL("SELECT identificadorTrabajor FROM {}.DirectorVuelo").format(sql.Identifier(schema_name)))
        director_vuelo_ids = [row[0] for row in cur.fetchall()]

        # Inserta registros aleatorios en la tabla IngenieroVuelo
        for _ in range(num_records):
            campo_trabajo = fake.word()

            # Asegúrate de que haya identificadores de trabajadores en DirectorVuelo disponibles
            if not director_vuelo_ids:
                break

            # Selecciona un identificador de trabajador existente en DirectorVuelo
            id_director_vuelo = fake.random_element(elements=director_vuelo_ids)

            # Selecciona atributos específicos del trabajador en DirectorVuelo
            cur.execute("""
                SELECT identificadorTrabajor, image, fechaNacimiento, genero, dni, fechaContratacion, salario, firstName, lastName
                FROM {}.DirectorVuelo
                WHERE identificadorTrabajor = %s
            """.format(schema_name), (id_director_vuelo,))

            # Obtiene los atributos del trabajador en DirectorVuelo
            atributos_director_vuelo = cur.fetchone()

            # Imprime los datos antes de la inserción en IngenieroVuelo
            #print(f"Insertando en IngenieroVuelo: {campo_trabajo}")

            # Ejecuta la inserción en IngenieroVuelo
            cur.execute("""
                INSERT INTO {}.IngenieroVuelo (
                    campoTrabajo,
                    Id_Director_Vuelo,
                    identificadorTrabajor,
                    image,
                    fechaNacimiento,
                    genero,
                    dni,
                    fechaContratacion,
                    salario,
                    firstName,
                    lastName
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
            """.format(schema_name), (
                campo_trabajo,
                id_director_vuelo,
                *atributos_director_vuelo
            ))

        # Confirma la transacción
        conn.commit()

        print("     7.Conexión exitosa y las inserciones en la tabla IngenieroVuelo se realizaron correctamente.")

    except Exception as e:
        print(f"Error: {e}")

def poblar_administrador(conn, num_records):
    def generar_valor_aleatorio():
        return fake.random_int(min=0, max=100)

    try:
        # Abre un cursor para ejecutar consultas SQL
        cur = conn.cursor()

        # Especifica el esquema para las inserciones
        schema_name = "millon_datos"

        # Obtén la lista de identificadores de trabajadores existentes
        cur.execute(sql.SQL("SELECT identificadorTrabajor FROM {}.Trabajador").format(sql.Identifier(schema_name)))
        trabajador_ids = [row[0] for row in cur.fetchall()]

        # Inserta registros aleatorios en la tabla Administrador
        for _ in range(num_records):
            email = fake.email()
            password = fake.password()
            adress = fake.address()
            phone = fake.random_int(min=100000000, max=999999999)
            role = fake.random_element(elements=('admin', 'superadmin'))

            # Asegúrate de que haya identificadores de trabajadores disponibles
            if not trabajador_ids:
                break

            # Selecciona un identificador de trabajador existente
            identificador_trabajador = fake.random_element(elements=trabajador_ids)

            # Elimina el identificador seleccionado para evitar duplicados
            trabajador_ids.remove(identificador_trabajador)

            # Selecciona atributos específicos del trabajador
            cur.execute("""
                SELECT image, fechaNacimiento, genero, dni, fechaContratacion, salario, firstName, lastName
                FROM {}.Trabajador
                WHERE identificadorTrabajor = %s
            """.format(schema_name), (identificador_trabajador,))

            # Obtiene los atributos del trabajador
            atributos_trabajador = cur.fetchone()

            # Imprime los datos antes de la inserción en Administrador
            #print(f"Insertando en Administrador: {email}, {password}, {adress}, {phone}, {role}")

            # Ejecuta la inserción en Administrador
            cur.execute("""
                INSERT INTO {}.Administrador (
                    email,
                    password,
                    adress,
                    phone,
                    role,
                    identificadorTrabajor,
                    image,
                    fechaNacimiento,
                    genero,
                    dni,
                    fechaContratacion,
                    salario,
                    firstName,
                    lastName
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
            """.format(schema_name), (
                email,
                password,
                adress,
                phone,
                role,
                identificador_trabajador,
                *atributos_trabajador
            ))

        # Confirma la transacción
        conn.commit()

        print("     8.Conexión exitosa y las inserciones en la tabla Administrador se realizaron correctamente.")

    except Exception as e:
        print(f"Error: {e}")

def poblar_astronauta(conn, num_records):
    def generar_valor_aleatorio():
        return fake.random_int(min=0, max=100)

    try:
        # Abre un cursor para ejecutar consultas SQL
        cur = conn.cursor()

        # Especifica el esquema para las inserciones
        schema_name = "millon_datos"

        # Obtén la lista de identificadores de trabajadores existentes
        cur.execute(sql.SQL("SELECT identificadorTrabajor FROM {}.Trabajador").format(sql.Identifier(schema_name)))
        trabajador_ids = [row[0] for row in cur.fetchall()]

        # Obtén la lista de identificadores de misiones existentes
        cur.execute(sql.SQL("SELECT identificadorMision FROM {}.Mision").format(sql.Identifier(schema_name)))
        mision_ids = [row[0] for row in cur.fetchall()]

        # Obtén la lista de identificadores de directores de vuelo existentes
        cur.execute(sql.SQL("SELECT identificadorTrabajor FROM {}.DirectorVuelo").format(sql.Identifier(schema_name)))
        director_vuelo_ids = [row[0] for row in cur.fetchall()]

        # Obtén la lista de identificadores de astronautas existentes
        cur.execute(sql.SQL("SELECT identificadorTrabajor FROM {}.Astronauta").format(sql.Identifier(schema_name)))
        astronauta_ids = [row[0] for row in cur.fetchall()]

        # Inserta registros aleatorios en la tabla Astronauta
        for _ in range(num_records):
            campo = fake.word()

            # Asegúrate de que haya identificadores de trabajadores, misiones, directores de vuelo y astronautas disponibles
            if not trabajador_ids or not mision_ids or not director_vuelo_ids:
                break

            # Selecciona un identificador de trabajador existente que no esté en la lista de astronautas
            identificador_trabajador = fake.random_element(elements=set(trabajador_ids) - set(astronauta_ids))

            # Elimina el identificador seleccionado para evitar duplicados
            trabajador_ids.remove(identificador_trabajador)

            # Selecciona un identificador de misión existente
            id_mision = fake.random_element(elements=mision_ids)

            # Elimina el identificador seleccionado para evitar duplicados
            mision_ids.remove(id_mision)

            # Selecciona un identificador de director de vuelo existente
            id_director_vuelo = fake.random_element(elements=director_vuelo_ids)

            # Elimina el identificador seleccionado para evitar duplicados
            director_vuelo_ids.remove(id_director_vuelo)

            # Obtiene los atributos heredados del trabajador
            cur.execute("""
                SELECT image, fechaNacimiento, genero, dni, fechaContratacion, salario, firstName, lastName
                FROM {}.Trabajador
                WHERE identificadorTrabajor = %s
            """.format(schema_name), (identificador_trabajador,))

            # Obtiene los atributos del trabajador
            atributos_trabajador = cur.fetchone()

            # Imprime los datos antes de la inserción en Astronauta
            #print(f"Insertando en Astronauta: {campo}, {identificador_trabajador}, {id_mision}, {id_director_vuelo}")

            # Ejecuta la inserción en Astronauta
            cur.execute("""
                INSERT INTO {}.Astronauta (
                    campo,
                    identificadorTrabajor,
                    IdCohete,
                    Id_Director_Vuelo,
                    image,
                    fechaNacimiento,
                    genero,
                    dni,
                    fechaContratacion,
                    salario,
                    firstName,
                    lastName
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
            """.format(schema_name), (
                campo,
                identificador_trabajador,
                id_mision,
                id_director_vuelo,
                *atributos_trabajador
            ))

            # Agrega el identificador a la lista de astronautas
            astronauta_ids.append(identificador_trabajador)

        # Confirma la transacción
        conn.commit()

        print("     9.Conexión exitosa y las inserciones en la tabla Astronauta se realizaron correctamente.")

    except Exception as e:
        print(f"Error: {e}")

def poblar_componente_cohete(conn, num_records):
    def generar_valor_aleatorio():
        return fake.random_int(min=0, max=100)

    try:
        # Abre un cursor para ejecutar consultas SQL
        cur = conn.cursor()

        # Especifica el esquema para las inserciones
        schema_name = "millon_datos"

        # Obtén la lista de identificadores de cohetes existentes
        cur.execute(f"SELECT idCohete FROM {schema_name}.Cohete")
        cohete_ids = [row[0] for row in cur.fetchall()]

        # Inserta registros aleatorios en la tabla ComponenteCohete
        for _ in range(num_records):
            id_cohete = fake.random_element(elements=cohete_ids)
            tipo = fake.word()
            cantidad_requerida = fake.random_int(min=1, max=10)
            nombre = fake.word()

            # Imprime los datos antes de la inserción
            #print(f"Insertando en ComponenteCohete: {id_cohete}, {tipo}, {cantidad_requerida}, {nombre}")

            # Ejecuta la inserción en ComponenteCohete
            cur.execute(f"""
                INSERT INTO {schema_name}.ComponenteCohete (
                    idCohete,
                    tipo,
                    CantidadRequerida,
                    nombre
                ) VALUES (
                    %s, %s, %s, %s
                )
            """, (
                id_cohete,
                tipo,
                cantidad_requerida,
                nombre
            ))

        # Confirma la transacción
        conn.commit()

        print("     10.Conexión exitosa y las inserciones en la tabla ComponenteCohete se realizaron correctamente.")

    except Exception as e:
        print(f"Error: {e}")

def poblar_participa(conn, num_records):
    try:
        # Abre un cursor para ejecutar consultas SQL
        cur = conn.cursor()

        # Especifica el esquema para las inserciones
        schema_name = "millon_datos"

        # Obtén la lista de identificadores de astronautas existentes
        cur.execute(f"SELECT identificadorTrabajor FROM {schema_name}.Astronauta")
        astronauta_ids = [row[0] for row in cur.fetchall()]

        # Obtén la lista de identificadores de misiones existentes
        cur.execute(f"SELECT identificadorMision FROM {schema_name}.Mision")
        mision_ids = [row[0] for row in cur.fetchall()]

        # Inserta registros aleatorios en la tabla participa
        for _ in range(num_records):
            id_astronauta = fake.random_element(elements=astronauta_ids)
            id_mision = fake.random_element(elements=mision_ids)

            # Verifica si el par ya existe en la tabla
            cur.execute(f"SELECT * FROM {schema_name}.participa WHERE idTrabajador_Astronauta = %s AND idMision = %s", (id_astronauta, id_mision))
            existing_record = cur.fetchone()

            if not existing_record:
                # Imprime los datos antes de la inserción
                #print(f"Insertando en participa: {id_astronauta}, {id_mision}")

                # Ejecuta la inserción en participa
                cur.execute(f"""
                    INSERT INTO {schema_name}.participa (
                        idTrabajador_Astronauta,
                        idMision
                    ) VALUES (
                        %s, %s
                    )
                """, (
                    id_astronauta,
                    id_mision
                ))

        # Confirma la transacción
        conn.commit()

        print("     11.Conexión exitosa y las inserciones en la tabla participa se realizaron correctamente.")

    except Exception as e:
        print(f"Error: {e}")


def menu():
    while True:
        print(" --------------------- MENU -----------------------------------------")
        print()
        print("     1. Poblar la base de datos")
        print("     2. Salir")

        opcion = input("Selecciona una opción: ")

        if opcion == "1":
            # Ejecutar el poblado de cada tabla en el orden específico
            try:
                start_time = time.time()
                conn = conectar()
                num_registros = int(input("Ingrese la cantida de registros que desea agregarle a su base de datos:  "))
                print("\n-------------⚡Panel de Resultados⚡------------\n")
                poblar_trabajador(conn, num_registros)
                poblar_planeta(conn, num_registros)
                poblar_mision(conn, num_registros)
                poblar_cohete(conn, num_registros)
                poblar_lanzamiento(conn, num_registros)
                poblar_directorvuelo(conn, num_registros)
                poblar_ingeniero_vuelo(conn, num_registros)
                poblar_administrador(conn, num_registros)
                poblar_astronauta(conn, num_registros)
                poblar_componente_cohete(conn, num_registros)
                poblar_participa(conn, num_registros)
                elapsed_time = time.time() - start_time
                minutes, seconds = divmod(elapsed_time, 60)

                print()
                print(f"Tiempo transcurrido para llenar la Base de Datos: {int(minutes)} minutos y {round(seconds, 2)} segundos")
                print("\n\n")

                
            except Exception as e:
                print(f"Error: {e}")

        elif opcion == "2":
            conn.close()
            print("Saliendo...")
            break

        else:
            print("Opción no válida. Inténtalo de nuevo.")            

if __name__ == "__main__":
    menu()