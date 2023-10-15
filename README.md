#### EtL-проект по извлечению данных из справочника ТН ВЭД на сайте ФНС
#### О проекте
EtL-проект по извлечению данных из справочника "Товарная номенклатура внешнеэкономической деятельности" (ТНВЭД), их нормализации и загрузке в базу данных.
Источник данных: справочник ТН ВЭД размещённый на сайте Федеральной налоговой службы (https://www.nalog.gov.ru/rn77/program/5961290).

#### ПО и IT-технологии используемые в проекте:
* OS: Windows 10
* Язык программирования: Python 3.10
* Язык запросов: SQL
* СУБД: PostgreSQL 14.9
#### Схема EtL процесса
![EtL_scheme](https://github.com/DE-Alex/Tnved/assets/139635578/5a13c043-b8de-4de7-83ff-0848cb751f94)
---

##### Структура проекта
Этапы проекта реализованы в виде отдельных скриптов что позволяет легко вносить изменения и запускать их по отдельности.
Настройки источника данных, подключения к БД, названия таблиц БД и т.п. вынесены в конфигурационный файл (pipeline.conf) для упрощения настройки и дальнейшей поддержки ETL-проекта.
Ключевые файлы проекта:
- файл с настройками (pipeline.conf); 
- 3 скрипта с основным кодом (s1_extract.py, s2_normalize.py, s3_insert_db.py);
- 2 скрипта с часто выполняемыми операциями (s4_db_operations.py, s5_common_func.py);
- скрипт создающий объекты базы данных (create_objects.py).
Отчёт о выполнении всех этапов и возможные ошибки сохраняются в директории '/log' в файле журнала.

#### Окружение и зависимости
В проекте использовались стандартные модули Python, а также дополнительные модули:
- requests (загрузка данных с сайта)
- python-dateutil (работа с датами)
- psycopg (подключение и работа с Postgres)
Полный перечень модулей и их зависимостей для настройки окружения приведён в [requirements.txt](requirements.txt)

#### Возможное развитие
1. На данном этапе выполняется подготовка текста для размещения в базе данных: замена кодировки на utf-8, конвертация дат в формат ISO, исключение пустых строк и служебных символов.
   При необходимости можно продолжить нормализацию текстовой части справочника: убрать лишние пробелы и знаки пунктуации, привести нумерацию подпунктов к единому виду и т.д.
