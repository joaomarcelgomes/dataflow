CREATE TABLE IF NOT EXISTS weather_forecast (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    weekday VARCHAR(10) NOT NULL,
    max_temp INTEGER NOT NULL,
    min_temp INTEGER NOT NULL,
    humidity INTEGER NOT NULL,
    cloudiness REAL NOT NULL,
    rain REAL NOT NULL,
    rain_probability INTEGER NOT NULL,
    wind_speedy VARCHAR(20) NOT NULL,
    description VARCHAR(100) NOT NULL,
    condition VARCHAR(50) NOT NULL
);

CREATE TABLE IF NOT EXISTS weather_forecast_raw (
    id SERIAL PRIMARY KEY,
    full_date DATE NOT NULL,
    weekday VARCHAR(10) NOT NULL,
    temp_range VARCHAR(20) NOT NULL,
    humidity INTEGER NOT NULL,
    cloudiness REAL NOT NULL,
    rain REAL NOT NULL,
    rain_probability INTEGER NOT NULL,
    wind_speedy VARCHAR(20) NOT NULL,
    description VARCHAR(100) NOT NULL,
    condition VARCHAR(50) NOT NULL,
    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);



INSERT INTO weather_forecast (
    date, weekday, max_temp, min_temp, humidity, cloudiness,
    rain, rain_probability, wind_speedy, description, condition
) VALUES
('2025-07-15', 'Ter', 22, 13, 29, 0.0, 0.0, 0, '4.04 km/h', 'Tempo limpo', 'clear_day'),
('2025-07-16', 'Qua', 24, 12, 29, 0.0, 0.0, 0, '3.14 km/h', 'Tempo limpo', 'clear_day'),
('2025-07-17', 'Qui', 26, 14, 26, 46.0, 0.0, 0, '5.47 km/h', 'Parcialmente nublado', 'cloud'),
('2025-07-18', 'Sex', 26, 17, 28, 0.0, 0.0, 0, '5.93 km/h', 'Tempo limpo', 'clear_day'),
('2025-07-19', 'Sáb', 22, 14, 41, 1.0, 0.0, 0, '5.42 km/h', 'Tempo limpo', 'clear_day'),
('2025-07-20', 'Dom', 21, 11, 55, 60.0, 0.0, 0, '4.82 km/h', 'Tempo nublado', 'cloudly_day'),
('2025-07-21', 'Seg', 25, 13, 51, 99.0, 0.0, 0, '3.11 km/h', 'Tempo nublado', 'cloudly_day'),
('2025-07-22', 'Ter', 29, 17, 31, 0.0, 0.0, 0, '3.79 km/h', 'Tempo limpo', 'clear_day'),
('2025-07-23', 'Qua', 21, 16, 61, 36.0, 0.15, 20, '5.56 km/h', 'Chuvas esparsas', 'rain'),
('2025-07-24', 'Qui', 25, 14, 53, 7.0, 0.18, 20, '4.8 km/h', 'Chuvas esparsas', 'rain'),
('2025-07-25', 'Sex', 30, 16, 24, 2.0, 0.0, 0, '3.52 km/h', 'Tempo limpo', 'clear_day'),
('2025-07-26', 'Sáb', 30, 18, 22, 4.0, 0.0, 0, '5.51 km/h', 'Tempo limpo', 'clear_day'),
('2025-07-27', 'Dom', 21, 15, 89, 99.0, 1.11, 59, '5.31 km/h', 'Chuvas esparsas', 'rain'),
('2025-07-28', 'Seg', 23, 14, 65, 99.0, 0.27, 33, '5.48 km/h', 'Chuvas esparsas', 'rain'),
('2025-07-29', 'Ter', 28, 16, 32, 100.0, 0.0, 0, '4.44 km/h', 'Tempo nublado', 'cloudly_day');
