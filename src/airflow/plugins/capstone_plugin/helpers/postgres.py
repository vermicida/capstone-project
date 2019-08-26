class TableCreationQuery():

    weather_staging = """
        create table if not exists weather_staging (
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


class TableInsertionQuery():

    weather_staging = """
        insert into weather_staging (
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

    temperatures_by_month_and_city = """
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
