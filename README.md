# Geo summary

Скрипт анализирует данные о местонахождении устройств, определяет для каждого устройства адрес его первого или последнего местонахождения (в зависимости от указанного параметра командной строки) и сохраняет эту информацию в таблицу.

## Входные данные

Скрипт использует данные из следующих источников:
1. База данных MySQL - ID устройств.
2. База данных Oracle - данные о первом местоположении устройств.
3. База данных PostgreSQL с расширением PostGIS (starlinemaps) - таблицы OSM.
4. База данных PostgreSQL с расширением PostGIS (geo_sum) - предыдущие результаты работы скрипта.
5. База данных Redis - данные о последнем местоположении устройств.
6. Сервис TimezoneServer - данные о часовом поясе.

## Выходные данные

Результаты работы скрипта сохраняются в таблицу geo_summary базы данных PostgreSQL. Структура таблицы geo_summary приведена в файле "Geo_summary_table".

## Запуск скрипта

`python3 main.py [-c path] [-f] [-h]`

`-c path`	Путь к конфигурационному файлу. Шаблон конфигурационного файла представлен в файле "config_example.yaml".

`-f, --first`	Если ключ указан, то для каждого устройства скрипт определит адрес его первого местоположения, если не указан - то будет определяться адрес последнего местоположения для каждого устройства.

`-h, --help`	Справка.

## Сообщения в командной строке

При запуске скрипта выводится сообщение о выбранном режиме работы скрипта:
- `Insert first devices' locations.` - если скрипт был запущен с ключом -f.
- `Insert last devices' locations.` - если скрипт был запущен без ключа -f.

Во время работы скрипта отображается процент обработанных устройств:

- `Progress: {X}% complete`

При корректном завершении скрипта отображается количество новых записей в таблице geo_summary, количество возникших при обработке некритических ошибок, а также время работы скрипта:
- `Inserted {X} rows. {Y} errors occurred.` - если скрипт был запущен с ключом -f.
- `Inserted {X} rows. {Y} devices haven't changed their location. {Z} errors occurred.` - если скрипт был запущен без ключа -f.
- `Runtime of the program is {X} hours.`

В случае возникновения ошибки, из-за которой дальнейшая работа скрипта невозможна, будет выведено соответствующее ошибке сообщение:
- `Failed to read configuration file. The error occurred: {X}`
- `Failed to connect to database. Details are in geo_summary_error.log.`
- `Failed to select data from database. Details are in geo_summary_error.log.`

## Журнал ошибок

Если во время работы скрипта возникает ошибка, информация о ней записывается в файл `geo_summary_error.log`. Записи в файле могут быть 3 типов:

- `[INFO]	message`, где `message` - сообщение о начале работы скрипта или его корректном завершении, аналогичные сообщениям в командной строке.
- `[ERROR]	message`, где `message` - информация о возникшей ошибке.
- `[CRITICAL]	message`, где `message` - информация о возникшей ошибке, из-за которой выполнение скрипта было остановлено.

## Алгоритм работы скрипта
![](https://github.com/DaryaPanarina/geo_summary/raw/master/Algorithm.jpg)



Алгоритм функции определения первого местоположения устройств:

![](https://github.com/DaryaPanarina/geo_summary/raw/master/First_loc_check_algorithm.jpg)



Алгоритм функции определения последнего местоположения устройств:

![](https://github.com/DaryaPanarina/geo_summary/raw/master/Last_loc_check_algorithm.jpg)

## Содержимое репозитория

1. Исходный код скрипта: `main.py`, `Connection.py`.
2. SQL-описание таблицы geo_summary: `Geo_summary_table.md`.
3. Примеры SQL-запросов к таблице geo_summary: `Select_queries.md`.
4. Пример конфигурационного файла: `config_example.yaml`.
5. Protobuf-структура данных, получаемых из Redis: `proto_storage.proto`.
	
  Генерация класса для работы с данными из Protobuf:
  
  `protoc -I=$SRC_DIR --python_out=$DST_DIR $SRC_DIR/gps_data.proto`
  
6. Используемые в скрипте модули: `requirements.txt`.
7. README.
