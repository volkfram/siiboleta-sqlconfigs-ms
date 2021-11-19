import psycopg2

TABLES_CONFIGURATION_ORIG = [
    {
        'name'      :   'genders',
        'json_path' :   ['res/json_files/genders.json'],
        'query'     :   '(id BIGINT PRIMARY KEY, name VARCHAR(32))'
    },
    {
        'name'      :   'deputies',
        'json_path' :   ['res/json_files/deputies.json'],
        'query'     :   '(id BIGINT PRIMARY KEY, name VARCHAR(32), name_2 VARCHAR(32), fathers_lastname VARCHAR(32), mothers_lastname VARCHAR(32), birthday DATE, rut VARCHAR(8), rut_dv VARCHAR(2), gender_id BIGINT REFERENCES genders(id))'
    },
    {
        'name'      :   'room_session_types',
        'json_path' :   ['res/json_files/room_session_types.json'],
        'query'     :   '(id BIGINT PRIMARY KEY, name VARCHAR(32))'
    },
    {
        'name'      :   'room_session_statuses',
        'json_path' :   ['res/json_files/room_session_statuses.json'],
        'query'     :   '(id BIGINT PRIMARY KEY, name VARCHAR(32))'
    },
    {
        'name'      :   'attendance_types',
        'json_path' :   ['res/json_files/attendance_types.json'],
        'query'     :   '(id BIGINT PRIMARY KEY, name VARCHAR(32))'
    },
    {
        'name'      :   'absence_reasons',
        'json_path' :   ['res/json_files/absence_reasons.json'],
        'query'     :   '(id BIGINT PRIMARY KEY, name VARCHAR(64), is_attendance_reductor BOOLEAN, is_quorum_reductor BOOLEAN)'
    },
    {
        'name'      :   'legislature_types',
        'json_path' :   ['res/json_files/legislature_types.json'],
        'query'     :   '(id BIGINT PRIMARY KEY, name VARCHAR(32))'
    },
    {
        'name'      :   'legislative_periods',
        'json_path' :   ['res/json_files/legislative_periods.json'],
        'query'     :   '(id BIGINT PRIMARY KEY, name VARCHAR(32), start_date TIMESTAMP, end_date TIMESTAMP)'
    },
    {
        'name'      :   'legislatures',
        'json_path' :   ['res/json_files/legislatures.json'],
        'query'     :   '(id BIGINT PRIMARY KEY, number BIGINT, start_date TIMESTAMP, end_date TIMESTAMP, legislature_type_id BIGINT REFERENCES legislature_types(id), legislative_period_id BIGINT REFERENCES legislative_periods(id))'
    },
    {
        'name'      :   'room_sessions',
        'json_path' :   ['res/json_files/room_sessions.json'],
        'query'     :   '(id BIGINT PRIMARY KEY, number BIGINT, start_date TIMESTAMP, end_date TIMESTAMP, room_session_type_id BIGINT REFERENCES room_session_types(id), room_session_status_id BIGINT REFERENCES room_session_statuses(id), legislature_id BIGINT REFERENCES legislatures(id))'
    },
    {
        'name'      :   'attendances',
        'json_path' :   ['res/json_files/attendances.json'],
        'query'     :   '(id BIGINT PRIMARY KEY, attendance_type_id BIGINT REFERENCES attendance_types(id), absence_reason_id BIGINT REFERENCES absence_reasons(id), room_session_id BIGINT REFERENCES room_sessions(id), deputy_id BIGINT REFERENCES deputies(id))'
    },
    {
        'name'      :   'ministries',
        'json_path' :   ['res/json_files/ministries.json'],
        'query'     :   '(id BIGINT PRIMARY KEY, name VARCHAR(64))'
    },
    {
        'name'      :   'subjects',
        'json_path' :   ['res/json_files/subjects.json'],
        'query'     :   '(id BIGINT PRIMARY KEY, name VARCHAR(256))'
    },
    {
        'name'      :   'origin_chambers',
        'json_path' :   ['res/json_files/origin_chambers.json'],
        'query'     :   '(id BIGINT PRIMARY KEY, name VARCHAR(32))'
    },
    {
        'name'      :   'law_project_initiative_types',
        'json_path' :   ['res/json_files/law_project_initiative_types.json'],
        'query'     :   '(id BIGINT PRIMARY KEY, name VARCHAR(32))'
    },
    
    {
        'name'      :   'law_projects',
        'json_path' :   ['res/json_files/law_projects.json'],
        'query'     :   '(id BIGINT PRIMARY KEY, bulletin_number VARCHAR(16), name TEXT, ingress_date TIMESTAMP, is_admissible BOOLEAN, origin_chamber_id BIGINT REFERENCES origin_chambers(id), law_project_initiative_type_id BIGINT REFERENCES law_project_initiative_types(id))'
    },
    {
        'name'      :   'law_projects_ministries',
        'json_path' :   ['res/json_files/law_projects_ministries.json'],
        'query'     :   '(id BIGINT PRIMARY KEY, law_project_id BIGINT REFERENCES law_projects(id), ministry_id BIGINT REFERENCES ministries(id))'
    },
    {
        'name'      :   'law_projects_subjects',
        'json_path' :   ['res/json_files/law_projects_subjects.json'],
        'query'     :   '(id BIGINT PRIMARY KEY, law_project_id BIGINT REFERENCES law_projects(id), subject_id BIGINT REFERENCES subjects(id))'
    },
    {
        'name'      :   'pparties',
        'json_path' :   ['res/json_files/pparties.json'],
        'query'     :   '(id BIGINT PRIMARY KEY, name VARCHAR(64), alias VARCHAR(32))'
    },
    {
        'name'      :   'commissions',
        'json_path' :   ['res/json_files/commissions.json'],
        'query'     :   '(id BIGINT PRIMARY KEY, name VARCHAR(256), web_name VARCHAR(256), alias VARCHAR(256), type VARCHAR(6), start_date DATE, constitution_date DATE, end_date DATE, email VARCHAR(64), phone VARCHAR(64), fax VARCHAR(64), number VARCHAR(64), legislative_period_id BIGINT REFERENCES legislative_periods(id))'
    },
    {
        'name'      :   'received_answers',
        'json_path' :   ['res/json_files/received_answers.json'],
        'query'     :   '(id BIGINT PRIMARY KEY, number VARCHAR(32), date TIMESTAMP, sender VARCHAR(256), sender_id VARCHAR(6))'
    },
    {
        'name'      :   'militancies',
        'json_path' :   ['res/json_files/militancies.json'],
        'query'     :   '(id BIGINT PRIMARY KEY, deputy_id BIGINT REFERENCES deputies(id), pparty_id BIGINT REFERENCES pparties(id), start_date TIMESTAMP, end_date TIMESTAMP)'
    },
    {
        'name'      :   'deputies_commissions',
        'json_path' :   ['res/json_files/deputies_commissions.json'],
        'query'     :   '(id BIGINT PRIMARY KEY, commission_id BIGINT REFERENCES commissions(id), deputy_id BIGINT REFERENCES deputies(id), start_date TIMESTAMP, end_date TIMESTAMP, member_type VARCHAR(16), is_current BOOLEAN)'
    },
    {
        'name'      :   'deputies_legislative_periods',
        'json_path' :   ['res/json_files/deputies_legislative_periods.json'],
        'query'     :   '(id BIGINT PRIMARY KEY, legislative_period_id BIGINT REFERENCES legislative_periods(id), deputy_id BIGINT REFERENCES deputies(id))'
    },
    {
        'name'      :   'nonlaw_project_statuses',
        'json_path' :   ['res/json_files/nonlaw_project_statuses.json'],
        'query'     :   '(id BIGINT PRIMARY KEY, name VARCHAR(32))'
    },
    {
        'name'      :   'nonlaw_projects',
        'json_path' :   ['res/json_files/nonlaw_projects.json'],
        'query'     :   '(id BIGINT PRIMARY KEY, nonlaw_project_status_id BIGINT REFERENCES nonlaw_project_statuses(id), legislative_period_id BIGINT REFERENCES legislative_periods(id), number VARCHAR(10), subject TEXT, type VARCHAR(16), ingress_date TIMESTAMP)'
    },
    {
        'name'      :   'sent_offices',
        'json_path' :   ['res/json_files/sent_offices.json'],
        'query'     :   '(id BIGINT PRIMARY KEY, nonlaw_project_id BIGINT REFERENCES nonlaw_projects(id), number VARCHAR(10), dispatch_date TIMESTAMP, delivery_date TIMESTAMP, receiver VARCHAR(128), receiver_id INT)'
    },
    {
        'name'      :   'authors',
        'json_path' :   ['res/json_files/authors.json'],
        'query'     :   '(id BIGINT PRIMARY KEY, deputy_id BIGINT REFERENCES deputies(id), nonlaw_project_id BIGINT REFERENCES nonlaw_projects(id), order_num INT)'
    },
    {
        'name'      :   'nonlaw_projects_received_answers',
        'json_path' :   ['res/json_files/nonlaw_projects_received_answers.json'],
        'query'     :   '(id BIGINT PRIMARY KEY, received_answer_id BIGINT REFERENCES received_answers(id), nonlaw_project_id BIGINT REFERENCES nonlaw_projects(id))'
    },
    {
        'name'      :   'project_categories',
        'json_path' :   ['res/json_files/project_categories.json'],
        'query'     :   '(id BIGINT PRIMARY KEY, name VARCHAR(16))'
    },
    {
        'name'      :   'projects',
        'json_path' :   ['res/json_files/projects.json'],
        'query'     :   '(id BIGINT PRIMARY KEY, law_project_id BIGINT REFERENCES law_projects(id), nonlaw_project_id BIGINT REFERENCES nonlaw_projects(id))'
    },
    {
        'name'      :   'constitutional_procedures',
        'json_path' :   ['res/json_files/constitutional_procedures.json'],
        'query'     :   '(id BIGINT PRIMARY KEY, name VARCHAR(32))'
    },
    {
        'name'      :   'regulatory_procedures',
        'json_path' :   ['res/json_files/regulatory_procedures.json'],
        'query'     :   '(id BIGINT PRIMARY KEY, name VARCHAR(32))'
    },
    {
        'name'      :   'law_project_voting_types',
        'json_path' :   ['res/json_files/law_project_voting_types.json'],
        'query'     :   '(id BIGINT PRIMARY KEY, name VARCHAR(32))'
    },
    {
        'name'      :   'voting_types',
        'json_path' :   ['res/json_files/voting_types.json'],
        'query'     :   '(id BIGINT PRIMARY KEY, name VARCHAR(32))'
    },
    {
        'name'      :   'voting_quorum_types',
        'json_path' :   ['res/json_files/voting_quorum_types.json'],
        'query'     :   '(id BIGINT PRIMARY KEY, name VARCHAR(32))'
    },
    {
        'name'      :   'voting_result_types',
        'json_path' :   ['res/json_files/voting_result_types.json'],
        'query'     :   '(id BIGINT PRIMARY KEY, name VARCHAR(32))'
    },
    {
        'name'      :   'votings',
        'json_path' :   ['res/json_files/votings.json'],
        'query'     :   '(id BIGINT PRIMARY KEY, description TEXT, date TIMESTAMP, total_yes VARCHAR(4), total_no VARCHAR(4), total_abstention VARCHAR(4), total_dispensed VARCHAR(4), voting_quorum_type_id BIGINT REFERENCES voting_quorum_types(id), voting_result_type_id BIGINT REFERENCES voting_result_types(id), voting_type_id BIGINT REFERENCES voting_types(id), project_id BIGINT REFERENCES projects(id))'
    },
    {
        'name'      :   'voting_extras',
        'json_path' :   ['res/json_files/voting_extras.json'],
        'query'     :   '(id BIGINT PRIMARY KEY, article TEXT, law_project_voting_type_id BIGINT REFERENCES law_project_voting_types(id), constitutional_procedure_id BIGINT REFERENCES constitutional_procedures(id), regulatory_procedure_id BIGINT REFERENCES regulatory_procedures(id), voting_id BIGINT REFERENCES votings(id))'
    },
    {
        'name'      :   'vote_types',
        'json_path' :   ['res/json_files/vote_types.json'],
        'query'     :   '(id BIGINT PRIMARY KEY, name VARCHAR(32))'
    },
    {
        'name'      :   'votes',
        'json_path' :   ['res/json_files/votes.json'],
        'query'     :   '(id BIGINT PRIMARY KEY, deputy_id BIGINT REFERENCES deputies(id), voting_id BIGINT REFERENCES votings(id), vote_type_id BIGINT REFERENCES vote_types(id))'
    }
]

TABLES_CONFIGURATION = [
    {
        'name'      :   'votes',
        'json_path' :   ['res/json_files/votes.json'],
        'query'     :   '(id BIGINT PRIMARY KEY, deputy_id BIGINT REFERENCES deputies(id), voting_id BIGINT REFERENCES votings(id), vote_type_id BIGINT REFERENCES vote_types(id))'
    }
]

def generateTables(connection, tables_info) :

    cursor = connection.cursor()

    for table_info in tables_info :
        try:
            query_create_table = 'CREATE TABLE ' + table_info['name'] + ' ' + table_info['query'] + ';'
            cursor.execute(query_create_table)
            connection.commit()
            print('[i] Table "' + table_info['name'] + '" created successfully!')

        except (Exception, psycopg2.Error) as error :
            print("[E] " + str(error))
            connection.rollback()

def destroyTables(connection, tables_info) :

    cursor = connection.cursor()

    for table_info in tables_info :
        try:
            query_drop_table = 'DROP TABLE ' + table_info['name'] + ' CASCADE;'
            cursor.execute(query_drop_table)
            connection.commit()
            print('[i] Table "' + table_info['name'] + '" dropped successfully!')

        except (Exception, psycopg2.Error) as error :
            print("[E] " + str(error))
            connection.rollback()

def executeQuery(connection, statement) :

    cursor = connection.cursor()

    try:
        cursor.execute(statement)
        connection.commit()
        print('[i] Query executed successfully!')

    except (Exception, psycopg2.Error) as error :
        print("[E] " + str(error))
        connection.rollback()

    finally:
        cursor.close()

def sqlDropTable(connection, table_name, is_cascade) :

    if is_cascade :
        query = 'DROP TABLE ' + table_name + ' CASCADE;'
    else :
        query = 'DROP TABLE ' + table_name + ';'

    executeQuery(connection, query)


def generateInsertStatement(table_name, table_rows) :
    
    column_names = '('
    column_values = ''

    table_keys = table_rows[0].keys()

    for indx, table_key in enumerate(table_keys) :
        column_names = column_names + str(table_key)
        if indx != len(table_keys) - 1 :
            column_names = column_names + ", "
        else :
            column_names = column_names + ")"

    for indx, table_row in enumerate(table_rows) : 
        for jndx, table_key in enumerate(table_keys) :

            new_value = str(table_row[table_key]).replace('\'', '\'\'')
            if new_value == '' :
                new_value = 'NULL'
            else :
                new_value = '\'' + new_value + '\''

            if jndx == 0 :
                column_values = column_values + '(' + new_value + ', '
            elif jndx != len(table_keys) - 1 :
                column_values = column_values + new_value + ','
            else :
                column_values = column_values + new_value +')'

        if indx != len(table_rows) - 1 :
            column_values = column_values + ', '

    full_string = 'INSERT INTO ' + table_name  + column_names + ' VALUES ' + column_values + ';'
    return full_string