CREATION_QUERIES = {

    'weather_staging': """
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
        );
    """,

    'temps_by_month': """
        create table if not exists temps_by_month (
            year int,
            month int,
            city text,
            tavg numeric,
            tmax numeric,
            tmin numeric,
            constraint monthly_weather_unique unique (year, month, city)
        );
    """,

    'temps_by_quarter': """
        create table if not exists temps_by_quarter (
            year int,
            quarter int,
            city text,
            tavg numeric,
            tmax numeric,
            tmin numeric,
            constraint quarterly_weather_unique unique (year, quarter, city)
        );
    """,

    'temps_by_year': """
        create table if not exists temps_by_year (
            year int,
            city text,
            tavg numeric,
            tmax numeric,
            tmin numeric,
            constraint yearly_weather_unique unique (year, city)
        );
    """
}

INSERTION_QUERIES = {

    'weather_staging': """
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
        );
    """,

    'temps_by_month': """
        insert into temps_by_month (
            year,
            month,
            city,
            tavg,
            tmax,
            tmin
        )
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
        on conflict on constraint monthly_weather_unique do nothing;
    """,

    'temps_by_quarter': """
        insert into temps_by_quarter (
            year,
            quarter,
            city,
            tavg,
            tmax,
            tmin
        )
          select anio,
                 quarter,
                 provincia,
                 avg(tmed) as tmed_prov,
                 max(tmax) as tmax_prov,
                 min(tmin) as tmin_prov
           from (
               select anio,
                      date_part('quarter', fecha)::integer quarter,
                      provincia,
                      tmed,
                      tmax,
                      tmin
                 from weather_staging
                where tmed != 'NAN'
                  and tmax != 'NAN'
                  and tmin != 'NAN'
           ) as t
        group by anio,
                 quarter,
                 provincia
        order by provincia
        on conflict on constraint quarterly_weather_unique do nothing;
    """,

    'temps_by_year': """
        insert into temps_by_year (
            year,
            city,
            tavg,
            tmax,
            tmin
        )
          select anio,
                 provincia,
                 avg(tmed) as tmed_prov,
                 max(tmax) as tmax_prov,
                 min(tmin) as tmin_prov
            from weather_staging
           where tmed != 'NAN'
             and tmax != 'NAN'
             and tmin != 'NAN'
        group by anio,
                 provincia
        order by provincia
        on conflict on constraint yearly_weather_unique do nothing;
    """
}
