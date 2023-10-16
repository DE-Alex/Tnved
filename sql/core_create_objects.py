actions = [  
            '''
            CREATE SCHEMA IF NOT EXISTS tn_ved_relative;
            '''
            # ,
            # '''
            # CREATE TABLE IF NOT EXISTS tn_ved_relative.tb_periods (
                # period_id SERIAL NOT NULL PRIMARY KEY,
                # time_from varchar NOT NULL,
                # expired2 varchar NULL             
            # );
            # '''
            ,
            '''
            CREATE TABLE IF NOT EXISTS tn_ved_relative.tb_tnved_version (
                table_name varchar NOT NULL,
                version varchar NOT NULL,
                date_from varchar NOT NULL,
                some_code varchar NULL,
                period_id int2 NOT NULL ,
                time_from varchar NOT NULL,
                expired2 varchar NULL,
                CONSTRAINT tb_tnved_version_pkey PRIMARY KEY (table_name, version, period_id)
            );
            '''
            ,
            '''
            CREATE TABLE IF NOT EXISTS tn_ved_relative.tb_razdel (
                razdel int2 UNIQUE NOT NULL,
                naim varchar NULL,
                prim varchar NULL,
                date_from varchar NOT NULL,
                expired varchar NULL,
                CONSTRAINT tb_razdel_pkey PRIMARY KEY (razdel, date_from)
            );
            '''
            ,
            '''
            CREATE TABLE IF NOT EXISTS tn_ved_relative.tb_gruppa (
                razdel int2 NULL,
                gruppa int2 UNIQUE NOT NULL,
                naim varchar NULL,
                prim varchar NULL,
                date_from varchar NOT NULL,
                expired varchar NULL,
                FOREIGN KEY (razdel)  REFERENCES tn_ved_relative.tb_razdel (razdel),
                CONSTRAINT tb_gruppa_pkey PRIMARY KEY (gruppa, date_from)
            );            
            '''
            ,
            '''
            CREATE TABLE IF NOT EXISTS tn_ved_relative.tb_tov_poz (
                gruppa int2 NOT NULL,
                tov_poz int2 UNIQUE NOT NULL,
                naim varchar NULL,
                date_from varchar NOT NULL,
                expired varchar NULL,
                FOREIGN KEY (gruppa)  REFERENCES tn_ved_relative.tb_gruppa (gruppa),
                CONSTRAINT tb_tov_poz_pkey PRIMARY KEY (gruppa, tov_poz, date_from)
            );             
            '''
            ,
            '''
            CREATE TABLE IF NOT EXISTS tn_ved_relative.tb_sub_poz (
                gruppa int2 NOT NULL,
                tov_poz int2 NOT NULL,
                sub_poz int4 NOT NULL,
                kr_naim varchar NULL,
                date_from varchar NOT NULL,
                expired varchar NULL,
                FOREIGN KEY (gruppa)  REFERENCES tn_ved_relative.tb_gruppa (gruppa),
                FOREIGN KEY (tov_poz)  REFERENCES tn_ved_relative.tb_tov_poz (tov_poz),
                CONSTRAINT tb_sub_poz_pkey PRIMARY KEY (gruppa, tov_poz, sub_poz, date_from)
            );
            '''
         ]
         
if __name__ == '__main__': 
	pass