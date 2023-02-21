SELECT of_db_drop_type('ofx_estatus_credito',	'CASCADE');

CREATE TABLE ofx_estatus_credito (
	credito         TEXT,
  producto        TEXT,
	cliente         TEXT,
  idrol           INTEGER,
  nombrerol       TEXT,
	nombre          TEXT,
	vin             TEXT,
	mto_inicial     NUMERIC,
	cuota           NUMERIC,
	cuota_ca        NUMERIC,
	cuota_total     NUMERIC,
	cta_2001        TEXT,
	sdo_2001_ant    NUMERIC,-- SALDO EN 2001 FECHA ANTERIOR
	fe_ant          DATE, 	-- FECHA ANTERIOR (INICIO DE RANGO DE CONSULTA)
	abo_2001_rgo    NUMERIC,-- ABONOS EN 2001 DE FECHA ANTERIOR A FECHA ACTUAL
	cgo_2001_rgo    NUMERIC,-- CARGOS EN 2001 DE FECHA ANTERIOR A FECHA ACTUAL
	variacion_saldo_2001 NUMERIC,	-- SUMA DE SALDOS EN 2001 DE FECHA ANTERIOR Y ABONOS HASTA AHORA
	sdo_2001_now    NUMERIC,-- SALDO DE CTA 2001 ACTUAL
	amortizacion    NUMERIC,	
  plazo           INTEGER,
  diasxplazo      INTEGER,
  tasaio          NUMERIC,
  plazosvencidos  NUMERIC,
  proximovence    DATE,
  fe_vence_cto    DATE,
  diasmora        NUMERIC,   
	-- monto_ideal     NUMERIC,
	abonoscredito   NUMERIC,
  aux             NUMERIC, -- vencido de accesorios
  aux2            NUMERIC, -- saldo vencido
	vencido         NUMERIC,
	saldo           TEXT
);

CREATE TYPE ofx_estatus_credito AS (
	credito         TEXT,
  producto        TEXT,
	cliente         TEXT,
  idrol           INTEGER,
  nombrerol       TEXT,
	nombre          TEXT,
	vin             TEXT,
	mto_inicial     NUMERIC,
	cuota           NUMERIC,
	cuota_ca        NUMERIC,
	cuota_total     NUMERIC,
	cta_2001        TEXT,
	sdo_2001_ant    NUMERIC,-- SALDO EN 2001 FECHA ANTERIOR
	fe_ant          DATE, 	-- FECHA ANTERIOR (INICIO DE RANGO DE CONSULTA)
	abo_2001_rgo    NUMERIC,-- ABONOS EN 2001 DE FECHA ANTERIOR A FECHA ACTUAL
	cgo_2001_rgo    NUMERIC,-- CARGOS EN 2001 DE FECHA ANTERIOR A FECHA ACTUAL
	variacion_saldo_2001 NUMERIC,	-- SUMA DE SALDOS EN 2001 DE FECHA ANTERIOR Y ABONOS HASTA AHORA
	sdo_2001_now    NUMERIC,-- SALDO DE CTA 2001 ACTUAL
	amortizacion    NUMERIC,	
  plazo           INTEGER,
  diasxplazo      INTEGER,
  tasaio          NUMERIC,
  plazosvencidos  NUMERIC,
  proximovence    DATE,
  fe_vence_cto    DATE,
  diasmora        NUMERIC,   
	-- monto_ideal     NUMERIC,
	abonoscredito   NUMERIC,
  aux             NUMERIC, -- vencido de accesorios
  aux2            NUMERIC, -- saldo vencido
	vencido         NUMERIC,
	saldo           TEXT
);

DROP FUNCTION estatus_credito(INTEGER, INTEGER, INTEGER, DATE, DATE);

CREATE OR REPLACE FUNCTION estatus_credito(INTEGER, INTEGER, INTEGER, DATE, DATE)
	-- RETURNS SETOF ofx_estatus_credito AS $$
	RETURNS VOID AS $$
DECLARE
  -- returns variable type table as object
  st ofx_estatus_credito%ROWTYPE;
  
  -- get values of argmuents in fucntion
  var_idsucaux    INTEGER := $1; -- variable fo the number credit
  var_idproducto  INTEGER := $2;
  var_idauxiliar  INTEGER := $3;
  var_fe_inicio      DATE := $4; -- variable for query start date
  var_fe_corte       DATE := $5; -- variable for query end date

  var_idsucauxref         INTEGER; -- variables to number of client
  var_idproductoref       INTEGER;
  var_idauxiliarref       INTEGER;
  _idpago         NUMERIC := 0.00; -- amortization number
  _mensualidad    NUMERIC := 0.00; -- amount month
  --_tipoprestamo   INTEGER := 0; -- unised variable
  -- variables of system 
  factor_iva_io   NUMERIC := of_params_get('/socios/productos/prestamos','iva_io');
  base_iva_io     NUMERIC := of_params_get('/socios/productos/prestamos','base_iva_io');

BEGIN
  -- initilization variables
	st.credito        := '';
  st.cliente        := '';
  st.nombre         := '';
  st.vin            := '';
  st.mto_inicial    := 0.00;
  st.cuota          := 0.00;
  st.cuota_ca       := 0.00;
  st.cuota_total    := 0.00;
  st.cta_2001       := '';
  st.sdo_2001_ant   := 0.00;  -- SALDO EN 2001 FECHA ANTERIOR
  st.abo_2001_rgo   := 0.00;  -- ABONOS EN 2001 DE FECHA ANTERIOR A FECHA ACTUAL
  st.cgo_2001_rgo   := 0.00;  -- CARGOS EN 2001 DE FECHA ANTERIOR A FECHA ACTUAL
  st.variacion_saldo_2001 := 0.00; -- SUMA DE SALDOS EN 2001 DE FECHA ANTERIOR Y ABONOS HASTA AHORA
  st.sdo_2001_now   := 0.00; -- SALDO DE CTA 2001 ACTUAL
  st.amortizacion   := 0.00;
  st.vencido        := 0.00;
  st.plazo           := 0.0;
  st.diasxplazo      := 0.0;
  st.tasaio          := 0.0;
  st.plazosvencidos  := 0.0;
  st.diasmora        := 0.0;  
  factor_iva_io     := ROUND((factor_iva_io / 100.00), 2);
  base_iva_io       := ROUND((base_iva_io / 100.00), 2);
  factor_iva_io     := factor_iva_io * base_iva_io;
  
  st.fe_ant = var_fe_inicio;

  -- RAISE NOTICE 'var_fe_inicio %',var_fe_inicio::TEXT;
  -- NUMERO DE CREDITO
  SELECT idsucaux || '-' || idproducto || '-' || idauxiliar
    INTO st.credito
    FROM deudores
   WHERE (idsucaux, idproducto, idauxiliar) = (var_idsucaux, var_idproducto, var_idauxiliar);

  -- NOMBRE DEL PRODUCTO
  SELECT nombre
    INTO st.producto
    FROM productos
   WHERE (idproducto) = (var_idproducto);

  -- CLIENTE
  SELECT idsucursal || '-' || idrol || '-' || idasociado
    INTO st.cliente
    FROM deudores
   WHERE (idsucaux, idproducto, idauxiliar) = (var_idsucaux, var_idproducto, var_idauxiliar);

  -- ROL Y NOMBRE DEL ROL
  SELECT idrol
    INTO st.idrol
    FROM deudores
   WHERE (idsucaux, idproducto, idauxiliar) = (var_idsucaux, var_idproducto, var_idauxiliar);
  
  SELECT nombre
    INTO st.nombrerol
    FROM roles
   WHERE idrol = st.idrol;

  -- NOMBRE
  SELECT of_nombre_asociado(d.idsucursal, d.idrol, d.idasociado)
    INTO st.nombre
    FROM deudores d
   WHERE (idsucaux, idproducto, idauxiliar) = (var_idsucaux, var_idproducto, var_idauxiliar);

  -- VIN
  SELECT ms.vin
    INTO st.vin
    FROM deudores d
          INNER JOIN ofx_multicampos_sustentable.auxiliar_masdatos ms USING(kauxiliar)
   WHERE (idsucaux, idproducto, idauxiliar) = (var_idsucaux, var_idproducto, var_idauxiliar);

-- MONTO OTORGADO
  SELECT montoentregado
    INTO st.mto_inicial
    FROM deudores d
   WHERE (idsucaux, idproducto, idauxiliar) = (var_idsucaux, var_idproducto, var_idauxiliar);

-- IDCUOTA ACTUAL
  SELECT idpago
    INTO _idpago 
    FROM planpago
   WHERE (idsucaux, idproducto, idauxiliar) = (var_idsucaux, var_idproducto, var_idauxiliar)
	        --AND of_periodo(vence)::INT = of_periodo(NOW()::DATE)::INT
          AND vence<=var_fe_corte 
          ORDER BY vence DESC
          LIMIT 1;

  st.amortizacion := _idpago;

  SELECT plazosvencidos, proximovenc, vence, diasmora
    INTO st.plazosvencidos, st.proximovence, st.fe_vence_cto, st.diasmora
    FROM cartera 
   WHERE (idsucaux, idproducto, idauxiliar) = (var_idsucaux, var_idproducto, var_idauxiliar)
	        AND fecha<=var_fe_corte 
          ORDER BY vence DESC
          LIMIT 1;

  SELECT tasaio, plazo, diasxplazo
    INTO st.tasaio, st.plazo, st.diasxplazo
    FROM deudores  
   WHERE (idsucaux, idproducto, idauxiliar) = (var_idsucaux, var_idproducto, var_idauxiliar);

  -- CUOTA ACTUAL
  SELECT of_si(of_iva_general(var_idsucaux, var_idproducto, var_idauxiliar, 1, now()::DATE),
              (round(abono + io, 2) + round((round(io, 2)* round(0.16, 2)), 2)), abono + io ) AS cuota,
	            ( ROUND(CAST(p.ca->'1'->>'monto' AS DECIMAL(8, 2)), 2) +
                ROUND((CAST(p.ca->'2'->>'monto' AS DECIMAL(8, 2))* 1.16), 2)+
                ROUND((CAST(p.ca->'3'->>'monto' AS DECIMAL(8, 2)) * 1.16), 2)+
                CAST(p.ca->'5'->>'monto' AS DECIMAL(8, 2))+
                ROUND(COALESCE(pe.io_incr, 0), 2)
		          ) AS cuota_ca
    INTO st.cuota, st.cuota_ca
    FROM planpago p
          INNER JOIN deudores d USING(idsucaux, idproducto, idauxiliar) --union por credito
          INNER JOIN asociados a USING(idsucursal, idrol, idasociado) -- union por cliente
          INNER JOIN directorio AS dir USING(idsucdir, iddir) -- union por claves unicas
          LEFT JOIN ppv.planpago_escalonado pe ON d.kauxiliar = pe.kauxiliar AND p.idpago = pe.idpago
   WHERE (idsucaux, idproducto, idauxiliar) = (var_idsucaux, var_idproducto, var_idauxiliar)
	        --AND of_periodo(vence)::INT = of_periodo(NOW()::DATE)::INT;
	        AND of_periodo(vence)::INT = of_periodo(var_fe_corte::DATE)::INT;

  -- 
  st.cuota_total := st.cuota + st.cuota_ca ;

  -- CUENTA 2001 y SALDO
  SELECT ar.idsucauxref || '-' || ar.idproductoref || '-' || ar.idauxiliarref, 
        --  of_auxiliar_saldo(ar.idsucauxref,ar.idproductoref, ar.idauxiliarref, now()::DATE)
         of_auxiliar_saldo(ar.idsucauxref,ar.idproductoref, ar.idauxiliarref, var_fe_corte::DATE)
    INTO st.cta_2001, 
         st.sdo_2001_now 
    FROM auxiliares_ref ar 
   WHERE (idsucaux, idproducto, idauxiliar) = (var_idsucaux, var_idproducto, var_idauxiliar)
	        AND idproductoref = 2001;

  SELECT ar.idsucauxref, ar.idproductoref, ar.idauxiliarref
    INTO var_idsucauxref, var_idproductoref, var_idauxiliarref 
    FROM auxiliares_ref ar
   WHERE (idsucaux, idproducto, idauxiliar) = (var_idsucaux, var_idproducto, var_idauxiliar)
	       AND idproductoref = 2001;

  -- SALDO EN CUENTA 2001 UN DIA ANTERIOR
  SELECT saldo --fecha, cargo, abono, saldo, secuencia
    INTO st.sdo_2001_ant
    FROM detalle_auxiliar da
   WHERE (idsucaux, idproducto, idauxiliar)=(var_idsucauxref, var_idproductoref, var_idauxiliarref)
	        AND fecha <= (var_fe_inicio::DATE)
	        ORDER BY fecha DESC , secuencia DESC LIMIT 1 ;
          --ORDER BY secuencia DESC LIMIT 1 ;

  -- OBTENER SALDO DE UN DIA ANTERIOR O ULTIMO SALDO
  --RAISE NOTICE 'SALDO DE CTA 2001 CON FECHA % -> %',(var_fe_inicio::DATE-1)::TEXT,  st.sdo_2001_ant::TEXT;

  -- ABONOS DE FECHA ANTERIOR A FECHA ACTUAL
  SELECT sum(da.abono)
    INTO st.abo_2001_rgo
    FROM detalle_auxiliar da
   WHERE (da.idsucaux, da.idproducto, da.idauxiliar) = (var_idsucauxref, var_idproductoref,	var_idauxiliarref)
	        AND da.fecha BETWEEN (var_fe_inicio::DATE)+1 AND var_fe_corte::DATE;
	        -- AND da.fecha BETWEEN (var_fe_inicio::DATE)+1 AND now()::DATE;
  --RAISE NOTICE 'ABONOS A PARTIR DE FECHA %', (var_fe_inicio::DATE)+1 ;

  -- CARGOS DE FECHA ANTERIOR A FECHA ACTUAL
  SELECT sum(cargo)
    INTO st.cgo_2001_rgo
    FROM detalle_auxiliar da
   WHERE (idsucaux, idproducto, idauxiliar) = (var_idsucauxref, var_idproductoref, var_idauxiliarref)
	        AND da.fecha BETWEEN (var_fe_inicio::DATE)+1 AND var_fe_corte::DATE ;
	        -- AND da.fecha BETWEEN (var_fe_inicio::DATE)+1 AND now()::DATE ;
  --RAISE NOTICE 'CARGOS A 2001 DESPUES DE % -> %',(var_fe_inicio::DATE)+1,st.cgo_2001_rgo::TEXT;

  -- SALDO EN 2001 DE FECHA ANTERIOR A FECHA ACTUAL
  -- el saldo abonado a 2001 durante el periodo de consulta hasta ahora considerando cargos y abonos
  SELECT sum(abono)-sum(cargo)
    INTO st.variacion_saldo_2001
    FROM detalle_auxiliar da 
   WHERE (idsucaux, idproducto, idauxiliar) = (var_idsucauxref, var_idproductoref, var_idauxiliarref)
	        AND da.fecha BETWEEN (var_fe_inicio::DATE)+1 AND var_fe_corte::DATE ;
	        -- AND da.fecha BETWEEN (var_fe_inicio::DATE)+1 AND now()::DATE ;
  --RAISE NOTICE 'ABONOS-CARGOS A 2001 DESPUES DE % -> %',var_fe_inicio::DATE::TEXT,st.variacion_saldo_2001::TEXT;

  -- SALDO EN 2001 FECHA ACTUAL 
  SELECT INTO st.sdo_2001_now of_auxiliar_saldo(var_idsucauxref, var_idproductoref, var_idauxiliarref, var_fe_corte::DATE);
  -- SELECT INTO st.sdo_2001_now of_auxiliar_saldo(var_idsucauxref, var_idproductoref, var_idauxiliarref, now()::DATE);

  --RAISE NOTICE 'SALDO EN 2001 AL % -> %',NOW()::DATE::TEXT,st.sdo_2001_now::TEXT;
  SELECT sum(abono + montoio + montoimp+montoca)
    INTO st.abonoscredito
    FROM detalle_auxiliar da
   WHERE (idsucaux, idproducto, idauxiliar) = (var_idsucaux, var_idproducto, var_idauxiliar) AND fecha BETWEEN  var_fe_inicio::DATE AND var_fe_corte::DATE;

  SELECT SUM(to_number(comision, '999G999G999G999D99')+
            to_number(iva_comision, '999G999G999G999D99')+
            to_number(segvida, '999G999G999G999D99')+
            to_number(segunidad, '999G999G999G999D99')+
            to_number(iva, '999G999G999G999D99')+
            to_number(gps, '999G999G999G999D99')+
            to_number(ivagps, '999G999G999G999D99')) INTO st.aux
    FROM ofx_reporte_cartera_credito (var_idsucaux, var_idproducto, var_idauxiliar, var_fe_corte);

  SELECT montovencido::NUMERIC + interes_total::NUMERIC
    INTO st.aux2
    FROM cartera 
   WHERE (idsucaux, idproducto, idauxiliar) = (var_idsucaux, var_idproducto, var_idauxiliar)
            ORDER BY fecha DESC LIMIT 1;  
            --AND fecha >= var_fe_inicio;
  
  -- TOTAL EXOGIBLE
  SELECT COALESCE(to_number(vencido, '999G999G999G999D99'),0)  INTO st.vencido
    FROM ofx_reporte_cartera_credito (var_idsucaux, var_idproducto, var_idauxiliar, var_fe_corte);
      
  SELECT adeudo_tot
    INTO st.saldo
    FROM ofx_reporte_cartera_credito(var_idsucaux, var_idproducto, var_idauxiliar, var_fe_corte);

  --RAISE NOTICE '%',st::TEXT;
  -- validate values null
  st.aux := COALESCE(st.aux, 0.0);
  st.aux2 := COALESCE(st.aux2, 0.0);
  st.abo_2001_rgo := COALESCE(st.abo_2001_rgo, 0);
  st.cgo_2001_rgo := COALESCE(st.cgo_2001_rgo, 0);
  st.variacion_saldo_2001 := COALESCE(st.variacion_saldo_2001, 0);
  st.sdo_2001_ant := COALESCE(st.sdo_2001_ant, 0);
  st.sdo_2001_ant := COALESCE(st.sdo_2001_ant, 0);
  st.abonoscredito := COALESCE(st.abonoscredito, 0);

  -- INSERT INTO
  INSERT INTO ofx_estatus_credito(credito, producto,	cliente,  idrol,
        nombrerol, nombre, vin, mto_inicial, cuota, cuota_ca, cuota_total,
        cta_2001, sdo_2001_ant, fe_ant, abo_2001_rgo, cgo_2001_rgo,
	      variacion_saldo_2001, sdo_2001_now, amortizacion,	 plazo,
        diasxplazo, tasaio, plazosvencidos, proximovence, fe_vence_cto,
        diasmora, abonoscredito, aux, aux2, vencido, saldo)
        VALUES
        (st.credito, st.producto,	st.cliente,  st.idrol,
        st.nombrerol, st.nombre, st.vin, st.mto_inicial, st.cuota, st.cuota_ca, st.cuota_total,
        st.cta_2001, st.sdo_2001_ant, st.fe_ant, st.abo_2001_rgo, st.cgo_2001_rgo,
	      st.variacion_saldo_2001, st.sdo_2001_now, st.amortizacion,	 st.plazo,
        st.diasxplazo, st.tasaio, st.plazosvencidos, st.proximovence, st.fe_vence_cto,
        st.diasmora, st.abonoscredito, st.aux, st.aux2, st.vencido, st.saldo);
  -- RETURN NEXT st;
  RETURN;

END
$$ LANGUAGE plpgsql;


SELECT 	estatus_credito(idsucaux, idproducto, idauxiliar, '01-02-2023'::DATE, '17-02-2023'::DATE) 
  FROM deudores d WHERE d.estatus = 3 AND idproducto = 5300 LIMIT 50;


--DEUDA:  SALDO MAS INTERESES MAS CAPITAL (1,2001,10349) 1,3405,55
-- ABONOS DE FECHA ANTERIOR A FECHA ACTUAL
SELECT estatus_credito(idsucaux, idproducto, idauxiliar, '31/12/2022'::DATE)
  FROM deudores d
 WHERE d.estatus = 3
	      AND idproducto = 3214
        LIMIT 100;

SELECT estatus_credito(1,3214,127,'31/12/2022'::DATE);

SELECT fecha, cargo, abono, saldo, secuencia, *
  FROM detalle_auxiliar da
 WHERE (idsucaux, idproducto, idauxiliar)=(1, 2001, 1931)
	      AND fecha <= ('31-12-2022'::DATE)
	      ORDER BY fecha DESC , secuencia DESC LIMIT 1 ;