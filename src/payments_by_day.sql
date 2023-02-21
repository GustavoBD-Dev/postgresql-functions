-- CREATE FUNCTION TO QUERY PAYMENTS BY DAY AND RANGE OR PRODUCTS
CREATE OR REPLACE FUNCTION abonos_cargos_x_fecha(DATE, DATE, INTEGER)
    -- Return object as table with the structure
    RETURNS TABLE(
    	abonos 		NUMERIC, 
    	fecha_dia 	   DATE,
        creditos 	INTEGER, 
        clientes 	INTEGER,
        promedioab  NUMERIC)
    LANGUAGE plpgsql
    AS 
$$
    DECLARE
        counter 	INTEGER := 0;  -- counter for he loop
        date_index 	DATE 	:= $1; -- index to loop query
       	date_row 	NUMERIC := 0;  -- store the values of sum of payments
        avgabonos   NUMERIC := 0;
        total_ctas  INTEGER := 0; -- count total creditos payments 
        total_ctes  INTEGER := 0;
        tipocredito INTEGER := $3;
        creditos_automotriz INTEGER[] := '{5300, 5200}';
        creditos_pymes   	INTEGER[] := '{3405, 3409}';
        creditos_todos 		INTEGER[] := creditos_automotriz || creditos_pymes;
        classifier  		INTEGER[];
    BEGIN
	    -- Set id  products for each classification
	    classifier  := (SELECT 
	    	CASE 
			    WHEN tipocredito = 0 THEN  creditos_todos 
	            WHEN tipocredito = 1 THEN  creditos_automotriz 
	            WHEN tipocredito = 2 THEN  creditos_pymes 
        	END);
        
        --RAISE NOTICE '%', classifier ;
	    -- validate dates
        IF ($1 < $2) THEN -- loop day by day from the first date until the last  
            WHILE
                date_index < $2::DATE -- variable to store the value day
            LOOP
                date_index := $1::DATE + counter; -- set date, value increment in loop
                
                -- query to get the sum of the payments in the variable date of the current cycle
                SELECT  sum(abono), avg(abonos)
                  INTO  date_row, avgabonos  
                  FROM  detalle_auxiliar da 
        				JOIN auxiliares_ref ar 
        					ON (da.idsucaux, da.idproducto, da.idauxiliar) = 
        					   (ar.idsucauxref, ar.idproductoref, ar.idauxiliarref)
        				JOIN deudores d 
        					ON (d.idsucaux, d.idproducto, d.idauxiliar) = 
        					   (ar.idsucaux, ar.idproducto, ar.idauxiliar)
 				 WHERE  da.fecha = date_index 
        				AND da.idproducto = 2001 
        				AND d.idproducto = ANY(classifier);
        			
                -- total creditos payments 
                SELECT  COUNT(DISTINCT(da.idsucaux  ||'-'|| da.idproducto  ||'-'|| da.idauxiliar)) ,
		                COUNT(DISTINCT(d.idsucursal  ||'-'|| d.idrol ||'-'|| d.idasociado))
                  INTO  total_ctas, total_ctes 
                  FROM  detalle_auxiliar da 
  		                JOIN auxiliares_ref ar ON (da.idsucaux, da.idproducto, da.idauxiliar) = (ar.idsucauxref, ar.idproductoref, ar.idauxiliarref)
  		                JOIN deudores d ON (d.idsucaux, d.idproducto, d.idauxiliar) = (ar.idsucaux, ar.idproducto, ar.idauxiliar)
                 WHERE  da.fecha = date_index
 		                AND da.idproducto = 2001 AND d.idproducto = ANY(classifier);
 		        -- show the data
                --RAISE NOTICE '(%) % % %', counter, date_row::TEXT, date_index::TEXT, $2::DATE;
                -- increment one day to variable date for the next query
                counter := counter + 1;
            -- set values of the varibles to return
            abonos 		:= COALESCE(date_row,0.0);
            fecha_dia 	:= date_index;
            promedioab  := COALESCE(ROUND(avgabonos,2), 0);
            creditos 	:= COALESCE(total_ctas, 0);
            clientes 	:= COALESCE(total_ctes, 0);
            -- return variables
            RETURN NEXT;
            END LOOP;
        ELSE
            RAISE NOTICE 'Verificar valor de argumentos, deben ser de tipo fecha';
        END IF;
    END;
$$;

DROP FUNCTION abonos_cargos_x_fecha(DATE, DATE, INTEGER);