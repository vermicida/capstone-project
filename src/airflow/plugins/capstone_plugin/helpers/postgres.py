class PostgresQuery():

    weather_creation = """
        create table if not exists weather (
            id serial primary key,
            fecha date,
            anio int,
            mes int,
            dia int,
            nombre text,
            provincia text,
            altitud numeric,
            tmed numeric,
            prec numeric,
            tmin numeric,
            tmax numeric,
            dir numeric,
            velmedia numeric,
            racha numeric,
            presMax numeric,
            presMin numeric
        )
    """

    weather_insertion = """
        insert into weather (
            fecha,
            anio,
            mes,
            dia,
            nombre,
            provincia,
            altitud,
            tmed,
            prec,
            tmin,
            tmax,
            dir,
            velmedia,
            racha,
            presMax,
            presMin
        )
        values (
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s
        )
    """

    daily_temps_by_city_selection = """
          select anio,
                 mes,
                 provincia,
                 avg(tmed) as tmed_prov,
                 max(tmax) as tmax_prov,
                 min(tmin) as tmin_prov
            from weather_staging
           where tmed != 'NAN'
             and tmax != 'NAN'
             and tmin != 'NAN'
        group by anio,
                 mes,
                 provincia
        order by provincia
    """
