/* Para la población de las tablas se crearon 4 esquemas: /*
/* - Esquema 1: mil_datos
   - Esquema 2: diez_mil_datos
   - Esquema 3: cien_mil_datos
   - Esquema 4: millon_datos
*/

/* ------------------------- Entidades ------------------------- */

/* Trabajador */
CREATE TABLE mil_datos.Trabajador(
    identificadorTrabajor SERIAL,
    image VARCHAR(50) NOT NULL,
    fechaNacimiento DATE NOT NULL,
    genero VARCHAR(9) NOT NULL,
    dni INT NOT NULL,
    fechaContratacion DATE NOT NULL,
    salario FLOAT NOT NULL,
    firstName VARCHAR(30) NOT NULL,
    lastName VARCHAR(30) NOT NULL
);

ALTER TABLE mil_datos.Trabajador ADD CONSTRAINT llaveprim_Trjd1k PRIMARY KEY (identificadorTrabajor);
ALTER TABLE mil_datos.Trabajador ADD CONSTRAINT Unique_DNI1k UNIQUE (dni);
ALTER TABLE mil_datos.Trabajador ADD CONSTRAINT CHK_fec_Ncm1k CHECK(
    EXTRACT(YEAR FROM AGE(NOW(), fechaNacimiento)) <= 70 AND
    EXTRACT(YEAR FROM AGE(NOW(),fechaNacimiento)) >= 25);
ALTER TABLE mil_datos.Trabajador ADD CONSTRAINT CHK_Sueldo1k CHECK ( salario>0 );
ALTER TABLE mil_datos.Trabajador ADD CONSTRAINT CHK_DNI1k CHECK ( DNI > 9999999 AND DNI < 100000000);
ALTER TABLE mil_datos.Trabajador ADD CONSTRAINT CHK_genero1k CHECK ( genero = 'masculino'
    or genero = 'femenino');

/* DirectorVuelo */
CREATE TABLE mil_datos.DirectorVuelo(
    ViajeISS INT NOT NULL,
    ViajeInterplanetario INT NOT NULL
) INHERITS (mil_datos.Trabajador) ;

ALTER TABLE millon_datos.DirectorVuelo ADD CONSTRAINT LLP_DV1M PRIMARY KEY (identificadortrabajor);
ALTER TABLE mil_datos.DirectorVuelo ADD CONSTRAINT CHK_UNQ_ID1k UNIQUE (identificadorTrabajor);
ALTER TABLE mil_datos.DirectorVuelo ADD CONSTRAINT CHK_ISS1k CHECK ( ViajeISS>=0 );
ALTER TABLE mil_datos.DirectorVuelo ADD CONSTRAINT CHK_VI1k CHECK ( ViajeInterplanetario >=0 );

/* IngenieroVuelo */
CREATE TABLE mil_datos.IngenieroVuelo(
        campoTrabajo VARCHAR(25) NOT NULL,
        Id_Director_Vuelo INT NOT NULL
) INHERITS (mil_datos.Trabajador);

ALTER TABLE millon_datos.IngenieroVuelo ADD CONSTRAINT LP_IV1M PRIMARY KEY (identificadortrabajor);
ALTER TABLE mil_datos.IngenieroVuelo ADD CONSTRAINT CHK_ID_IV1k FOREIGN KEY (Id_Director_Vuelo)
REFERENCES mil_datos.DirectorVuelo(identificadorTrabajor);

/* Administrador */
CREATE TABLE millon_datos.Administrador(
    email VARCHAR(50) NOT NULL,
    password VARCHAR(20) NOT NULL,
    adress VARCHAR(80) NOT NULL,
    phone BIGINT NOT NULL,
    role VARCHAR(30) NOT NULL
) INHERITS (millon_datos.Trabajador) ;

ALTER TABLE millon_datos.Administrador ADD CONSTRAINT LP_Ad1M PRIMARY KEY (identificadortrabajor);
ALTER TABLE millon_datos.Administrador ADD CONSTRAINT CHK_Num1M CHECK (phone>99999999 AND phone<1000000000);

/* Planeta */
CREATE TABLE diez_mil_datos.Planeta(
    identificarPlaneta SERIAL,
    gravedadPlaneta FLOAT NOT NULL,
    distancia FLOAT NOT NULL,
    habitado BOOLEAN NOT NULL,
    explorado BOOLEAN NOT NULL
);

ALTER TABLE diez_mil_datos.Planeta ADD CONSTRAINT llaveprim_IP10k PRIMARY KEY (identificarPlaneta);
ALTER TABLE diez_mil_datos.Planeta ADD CONSTRAINT CHK_GP10k CHECK ( gravedadPlaneta > 0 );
ALTER TABLE diez_mil_datos.Planeta ADD CONSTRAINT CHK_DST10k CHECK ( distancia > 0 );

/* Mision */
CREATE TABLE mil_datos.Mision(
    identificadorMision SERIAL,
    Id_Planeta INT NOT NULL,
    nombreMision VARCHAR(40) NOT NULL,
    cantidadPresupuesto FLOAT NOT NULL,
    inicioMision DATE NOT NULL,
    finMision DATE NOT NULL,
    objetivoMision VARCHAR(50) NOT NULL
);


ALTER TABLE mil_datos.Mision ADD CONSTRAINT llave_fk_Id_Planeta1k FOREIGN KEY (Id_Planeta)
REFERENCES mil_datos.Planeta(identificarPlaneta);
ALTER TABLE mil_datos.Mision ADD CONSTRAINT llaveprim_Msn1k PRIMARY KEY (identificadorMision);
ALTER TABLE mil_datos.Mision ADD CONSTRAINT CHK_CTD_PRSP1k CHECK (cantidadPresupuesto > 0);
ALTER TABLE mil_datos.Mision ADD CONSTRAINT CHK_DATE CHECK ( finMision > inicioMision );

/* Cohete */
CREATE TABLE mil_datos.Cohete(
    idCohete SERIAL,
    idMision INT NOT NULL,
    nombreCohete VARCHAR(30) NOT NULL,
    longitudCohete FLOAT NOT NULL,
    empuje FLOAT NOT NULL,
    pesoDespegue FLOAT NOT NULL,
    diametro FLOAT NOT NULL,
    velocidadMaxima FLOAT NOT NULL,
    cantidadEtapa INT NOT NULL,
    cargaUtil BIGINT NOT NULL
);

ALTER TABLE diez_mil_datos.Cohete ADD CONSTRAINT llavefk_Id_Mision10k FOREIGN KEY (idMision)
REFERENCES diez_mil_datos.Mision(identificadorMision);
ALTER TABLE diez_mil_datos.Cohete ADD CONSTRAINT llaveprim_Cht10k PRIMARY KEY(idCohete);
ALTER TABLE diez_mil_datos.Cohete ADD CONSTRAINT CHK_LC10k CHECK ( longitudCohete > 0 );
ALTER TABLE diez_mil_datos.Cohete ADD CONSTRAINT CHK_EMJ10k CHECK ( empuje > 0 );
ALTER TABLE diez_mil_datos.Cohete ADD CONSTRAINT CHK_PS_DPG10k CHECK ( pesoDespegue > 0);
ALTER TABLE diez_mil_datos.Cohete ADD CONSTRAINT CHK_DM10k CHECK ( diametro > 0 );
ALTER TABLE diez_mil_datos.Cohete ADD CONSTRAINT CHK_VM10k CHECK ( velocidadMaxima > 0 );
ALTER TABLE diez_mil_datos.Cohete ADD CONSTRAINT CHK_CT10k CHECK ( cantidadEtapa > 0 );
ALTER TABLE diez_mil_datos.Cohete ADD CONSTRAINT CHK_CU10k CHECK ( cargaUtil > 0 );

/* Astronauta */
CREATE TABLE millon_datos.Astronauta(
    campo VARCHAR(25) NOT NULL,
    Id_Director_Vuelo INT NOT NULL,
    IdCohete INT NOT NULL
) INHERITS (millon_datos.Trabajador);

ALTER TABLE millon_datos.Astronauta ADD CONSTRAINT LLP_Ast1M PRIMARY KEY (identificadortrabajor);
ALTER TABLE millon_datos.Astronauta ADD CONSTRAINT CHK_UNQ_Astro1M UNIQUE (identificadorTrabajor);
ALTER TABLE millon_datos.Astronauta ADD CONSTRAINT CHK_ID_DV1M FOREIGN KEY (Id_Director_Vuelo)
REFERENCES millon_datos.DirectorVuelo(identificadorTrabajor);
ALTER TABLE millon_datos.Astronauta ADD CONSTRAINT FK_1M FOREIGN KEY (idcohete)
REFERENCES millon_datos.Cohete(idcohete);


/* Lanzamiento */
CREATE TABLE diez_mil_datos.Lanzamiento(
    IdMision INT NOT NULL,
    idLanzamiento SERIAL,
    fechaLanzamiento DATE NOT NULL,
    lugarLanzamiento VARCHAR(50) NOT NULL
);

ALTER TABLE diez_mil_datos.Lanzamiento ADD CONSTRAINT llavefk_Id_Mision10k FOREIGN KEY (IdMision)
REFERENCES diez_mil_datos.Mision(identificadorMision);
ALTER TABLE diez_mil_datos.Lanzamiento ADD CONSTRAINT llaveprim_Lz10k PRIMARY KEY (idLanzamiento);

/* ComponenteCohete */
CREATE TABLE mil_datos.ComponenteCohete(
    idCohete INT NOT NULL,
    idComponente SERIAL,
    tipo VARCHAR(20) NOT NULL,
    CantidadRequerida BIGINT NOT NULL,
    nombre VARCHAR(30) NOT NULL
);

ALTER TABLE mil_datos.ComponenteCohete ADD CONSTRAINT llaveprim_CH1k PRIMARY KEY (idComponente, idCohete);
ALTER TABLE mil_datos.ComponenteCohete ADD CONSTRAINT llaveforan_CH1k FOREIGN KEY (idCohete)
REFERENCES mil_datos.Cohete(idCohete) ON DELETE CASCADE;
ALTER TABLE mil_datos.ComponenteCohete ADD CONSTRAINT CHK_Canti1k CHECK ( CantidadRequerida > 0 );


/* ------------------------- Relaciones ------------------------- */

/* participa */
CREATE TABLE mil_datos.participa(
    idTrabajador_Astronauta INT NOT NULL,
    idMision INT NOT NULL
);

ALTER TABLE mil_datos.participa ADD CONSTRAINT llavePrim_P1k PRIMARY KEY (idTrabajador_Astronauta,idMision);
ALTER TABLE mil_datos.participa ADD CONSTRAINT llavefk_P_id_Astro1k FOREIGN KEY (idTrabajador_Astronauta)
REFERENCES mil_datos.Astronauta(identificadorTrabajor);
ALTER TABLE mil_datos.participa ADD CONSTRAINT llavefk_P_id_Mis1k FOREIGN KEY (idMision)
REFERENCES mil_datos.Mision(identificadorMision);

-- Indices por defecto --

SET enable_mergejoin TO OFF;
SET enable_hashjoin TO OFF;
SET enable_bitmapscan TO OFF;
SET enable_sort TO OFF;

-- VACUUM FULL --

VACUUM FULL;


-- Consulta 1 || Indices --

-- 1. ¿Cuáles son las 20 misiones que han tenido mayor tiempo de duración? ¿En qué planetas se llevaron a cabo?
--¿En qué fecha se dieron sus lanzamientos (de forma descendente)?

CREATE INDEX IDX_ID_MSN_1M ON millon_datos.mision USING hash(identificadormision);
CREATE INDEX IDX_ID_PLT_1M ON millon_datos.planeta USING hash(identificarplaneta);
CREATE INDEX IDX_DATE_LZM_1M ON millon_datos.lanzamiento USING btree(fechalanzamiento);
CREATE INDEX IDX_DATE_I_MSN_1M ON millon_datos.mision USING btree(iniciomision);
CREATE INDEX IDX_DATE_F_MSN_1M ON millon_datos.mision USING btree(finmision);
CREATE INDEX IDX_FK_MSN_1M ON millon_datos.mision USING hash(id_planeta);
CREATE INDEX IDX_FK_PLT_1M ON millon_datos.lanzamiento USING hash(idmision);

DROP INDEX millon_datos.IDX_ID_MSN_1M;
DROP INDEX millon_datos.IDX_ID_PLT_1M;
DROP INDEX millon_datos.IDX_DATE_LZM_1M;
DROP INDEX millon_datos.IDX_DATE_I_MSN_1M;
DROP INDEX millon_datos.IDX_DATE_F_MSN_1M;
DROP INDEX millon_datos.IDX_FK_MSN_1M;
DROP INDEX millon_datos.IDX_FK_PLT_1M;

EXPLAIN (ANALYZE ) SELECT nombremision,
       (finMision - inicioMision) AS dias,
       identificarplaneta,
       fechalanzamiento
FROM (
    SELECT * FROM millon_datos.mision
    ORDER BY (finMision - inicioMision) DESC
    FETCH FIRST 20 ROWS ONLY
     ) AS more20 JOIN millon_datos.planeta
    ON more20.id_planeta = planeta.identificarplaneta
    JOIN millon_datos.lanzamiento
    ON more20.identificadormision = lanzamiento.idmision
ORDER BY (finMision - inicioMision) DESC, fechalanzamiento DESC;

-- Consulta 2 || Indices --

-- 2. ¿Qué cohetes tienen al menos 3 etapas? ¿En qué misiones han participado?
-- ¿A qué planeta se dirigieron? ¿Cuál es su componente (nombre y tipo) más requerido?

CREATE INDEX IDX_CHT_ETPS_1M ON millon_datos.cohete USING btree(cantidadetapa);
CREATE INDEX IDX_FK_CHT_MSN_1M ON millon_datos.cohete USING hash(idmision);
CREATE INDEX IDX_ID_MSN_1M ON millon_datos.mision USING hash(identificadormision);
CREATE INDEX IDX_ID_PLT_1M ON millon_datos.planeta USING hash(identificarplaneta);
CREATE INDEX IDX_FK_ID_PLT_1M ON millon_datos.mision USING hash(id_planeta);
CREATE INDEX IDK_ID_CHT_1M ON millon_datos.cohete USING hash(idcohete);
CREATE INDEX IDK_FK_ID_CHT_1M ON millon_datos.componentecohete USING hash(idcohete);
CREATE INDEX IDK_CNT_CMPCHT_1M ON millon_datos.componentecohete USING btree(cantidadrequerida);

DROP INDEX millon_datos.IDX_CHT_ETPS_1M;
DROP INDEX millon_datos.IDX_FK_CHT_MSN_1M;
DROP INDEX millon_datos.IDX_ID_MSN_1M;
DROP INDEX millon_datos.IDX_ID_PLT_1M;
DROP INDEX millon_datos.IDX_FK_ID_PLT_1M;
DROP INDEX millon_datos.IDK_ID_CHT_1M;
DROP INDEX millon_datos.IDK_FK_ID_CHT_1M;
DROP INDEX millon_datos.IDK_CNT_CMPCHT_1M;

EXPLAIN (ANALYZE ) SELECT nombrecohete,
       nombremision,
       identificarplaneta,
       moreRequired.nombre,
       moreRequired.tipo,
       moreRequired.cantidadrequerida
FROM (
    SELECT * FROM millon_datos.cohete
    WHERE cohete.cantidadetapa >= 3
     ) AS coheteMore3 JOIN millon_datos.mision
    ON coheteMore3.idmision = identificadormision
JOIN millon_datos.planeta
    ON id_planeta = identificarplaneta
JOIN(
    SELECT maxRequired.idcohete,
           maxRequired.cantidadrequerida,
           nombre,
           tipo
    FROM (
    SELECT idcohete, MAX(cantidadrequerida) AS cantidadrequerida
    FROM millon_datos.componentecohete
    GROUP BY idcohete
         ) AS maxRequired JOIN millon_datos.componentecohete
    ON maxRequired.idcohete = componentecohete.idcohete
           AND
       maxRequired.cantidadrequerida = componentecohete.cantidadrequerida
    ) AS moreRequired
    ON coheteMore3.idcohete = moreRequired.idcohete;

-- Consulta 3 || Indices --
-- 3. ¿Cuáles son los astronautas que han participado en las 20 misiones de mayor duración y a qué planetas
-- se dirigieron (incluyendo la distancia de viaje)?

CREATE INDEX IDX_MSN_F_1M ON millon_datos.mision USING btree(finmision);
CREATE INDEX IDX_MSN_I_1M ON millon_datos.mision USING btree(iniciomision);
CREATE INDEX IDX_ID_MSN_1M ON millon_datos.mision USING hash(identificadormision);
CREATE INDEX IDX_FK_ID_PTC_1M ON millon_datos.participa USING hash(idmision);
CREATE INDEX IDX_ID_PLT_1M ON millon_datos.planeta USING hash(identificarplaneta);
CREATE INDEX IDX_FK_ID_MSN_1M ON millon_datos.mision USING hash(id_planeta);
CREATE INDEX IDX_ID_ASTRO_1M ON millon_datos.astronauta USING hash(identificadortrabajor);
CREATE INDEX IDX_FK_ID_PTC2_1M ON millon_datos.participa USING hash(idtrabajador_astronauta);

DROP INDEX millon_datos.IDX_MSN_F_1M;
DROP INDEX millon_datos.IDX_MSN_I_1M;
DROP INDEX millon_datos.IDX_ID_MSN_1M;
DROP INDEX millon_datos.IDX_FK_ID_PTC_1M;
DROP INDEX millon_datos.IDX_ID_PLT_1M;
DROP INDEX millon_datos.IDX_FK_ID_MSN_1M;
DROP INDEX millon_datos.IDX_ID_ASTRO_1M;
DROP INDEX millon_datos.IDX_FK_ID_PTC2_1M;

EXPLAIN (ANALYZE ) SELECT astronauta.firstname,
       astronauta.lastname,
       moreDuration.nombremision,
       (finMision - inicioMision) AS dias,
       planeta.identificarplaneta,
       planeta.distancia
FROM millon_datos.astronauta JOIN (
    SELECT * FROM
    (
    SELECT *
    FROM millon_datos.mision
    ORDER BY (finMision - inicioMision) DESC
    FETCH FIRST 20 ROWS ONLY
    ) AS more20 JOIN millon_datos.participa ON more20.identificadormision = participa.idmision
                     ) AS moreDuration
    ON astronauta.identificadortrabajor = moreDuration.idtrabajador_astronauta
JOIN millon_datos.planeta ON moreDuration.id_planeta = planeta.identificarplaneta;

--Consulta 4 || Indices --
-- 4. ¿Cuáles astronautas han participado en las 20 misiones con mayor presupuesto y en qué cohetes han estado?


CREATE INDEX IDX_ID_MSN_1M ON millon_datos.mision USING hash(identificadormision);
CREATE INDEX IDX_FK_ID_PTC_1M ON millon_datos.participa USING hash(idmision);
CREATE INDEX IDX_ID_ASTRO_1M ON millon_datos.astronauta USING hash(identificadortrabajor);
CREATE INDEX IDX_FK_ID_PTC2_1M ON millon_datos.participa USING hash(idtrabajador_astronauta);
CREATE INDEX IDX_FK_ID_ASTRO_1M ON millon_datos.astronauta USING hash(idcohete);
CREATE INDEX IDX_ID_CHT_1M ON millon_datos.cohete USING hash(idcohete);
CREATE INDEX IDX_CANTPRESU_MSN_1M ON millon_datos.mision USING btree(cantidadpresupuesto);

DROP INDEX millon_datos.IDX_ID_MSN_1M;
DROP INDEX millon_datos.IDX_FK_ID_PTC_1M;
DROP INDEX millon_datos.IDX_ID_ASTRO_1M;
DROP INDEX millon_datos.IDX_FK_ID_PTC2_1M;
DROP INDEX millon_datos.IDX_FK_ID_ASTRO_1M;
DROP INDEX millon_datos.IDX_ID_CHT_1M;
DROP INDEX millon_datos.IDX_CANTPRESU_MSN_1M;

EXPLAIN (ANALYZE ) SELECT firstname,
       lastname,
       more20.nombremision,
       more20.cantidadpresupuesto ,
       cohete.nombreCohete
FROM (
    SELECT * FROM millon_datos.mision
    ORDER BY cantidadpresupuesto DESC
    FETCH FIRST 20 ROWS ONLY
     ) AS more20 JOIN millon_datos.participa
    ON more20.identificadormision = participa.idmision
JOIN millon_datos.astronauta
    ON participa.idtrabajador_astronauta = astronauta.identificadortrabajor
JOIN millon_datos.cohete
    ON astronauta.idcohete = cohete.idcohete;