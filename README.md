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
![EtL_scheme](https://github.com/DE-Alex/Tnved/assets/139635578/d098b79d-7694-4523-aa25-4a8f04fa837c)
---

##### Структура проекта
Этапы проекта реализованы в виде отдельных скриптов что позволяет легко вносить изменения и запускать их по отдельности.
Основные настройки источника данных, подключения к БД, названия таблиц БД и т.п. вынесены в конфигурационный файл (pipeline.conf).

Полученные из источника данные после обработки и нормализации размещаются в "Stage Layer" БД (схема "tn_ved") в виде несвязанных между собой таблиц (скрипт s3_insert_db.py).
С ними можно работать.

На следующем этапе в "Core Layer" (схема "tn_ved_relative") создается модель данных учитывающая связи между таблицами (скрипт s4_data_model.py, ветка [develop](https://github.com/DE-Alex/Tnved/tree/develop)).

Ключевые файлы проекта:
- файл с настройками (pipeline.conf); 
- 3 скрипта с основным кодом (s1_extract.py, s2_normalize.py, s3_insert_db.py);
- 2 скрипта с часто выполняемыми операциями (s4_db_operations.py, s5_common_func.py);
- скрипты создающие объекты базы данных (stage_create_objects.py и core_create_objects.py).
Отчёт о выполнении всех этапов и возможные ошибки сохраняются в директории '/log' в файле журнала.

#### Окружение и зависимости
В проекте использовались стандартные модули Python, а также дополнительные модули:
- requests (загрузка данных с сайта)
- python-dateutil (работа с датами)
- SQLAlchemy (работа с базой данных)
- pandas (загрузка/выгрузка, преобразование данных)
- psycopg (подключение и работа с Postgres)

  Полный перечень модулей и их зависимостей для настройки окружения приведён в [requirements.txt](requirements.txt)

#### Планируемые доработки
1. ~~Доделать модель данных~~ - выполнено
2. ~~Добавить функционал модуля pandas~~ - выполнено
3. Провести тестирование в OS Linux.
4. На данном этапе выполняется подготовка текста для размещения в базе данных: замена кодировки на utf-8, конвертация дат в формат ISO, исключение пустых строк и служебных символов.
   При необходимости можно продолжить нормализацию текстовой части справочника: убрать лишние пробелы и знаки пунктуации, привести нумерацию подпунктов к единому виду и т.д.
