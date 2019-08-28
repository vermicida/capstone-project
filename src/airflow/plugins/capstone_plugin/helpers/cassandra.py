class CassandraQuery():

    daily_temps_by_city_creation = """
        create table if not exists daily_temps_by_city (
            year int,
            month int,
            day int,
            city text,
            tavg float,
            tmax float,
            tmin float,
            primary key ((year, month, day), city)
        )
    """

    weekly_temps_by_city_creation = """
        create table if not exists weekly_temps_by_city (
            year int,
            month int,
            week int,
            city text,
            tavg float,
            tmax float,
            tmin float,
            primary key ((year, month, week), city)
        )
    """

    monthly_temps_by_city_creation = """
        create table if not exists monthly_temps_by_city (
            year int,
            month int,
            city text,
            tavg float,
            tmax float,
            tmin float,
            primary key ((year, month), city)
        )
    """

    quarterly_temps_by_city_creation = """
        create table if not exists quarterly_temps_by_city (
            year int,
            quarter int,
            city text,
            tavg float,
            tmax float,
            tmin float,
            primary key ((year, quarter), city)
        )
    """

    yearly_temps_by_city_creation = """
        create table if not exists yearly_temps_by_city (
            year int,
            city text,
            tavg float,
            tmax float,
            tmin float,
            primary key ((year), city)
        )
    """
