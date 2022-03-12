from sql import connector

def parse_into_json(soup_content, keyword, identifier):
    scripts = soup_content.find_all("script")

    string_with_json_obj = ""

    for element in scripts:
        if keyword in element.string:
            string_with_json_obj = element.string.strip()
            break
    try:
        ind_start = string_with_json_obj.index("('") + 2
        ind_end = string_with_json_obj.index("')")
        json_data = string_with_json_obj[ind_start:ind_end]
        json_data = json_data.encode('utf8').decode('unicode_escape')
    except:
        print(identifier)
    return json_data

def truncate_table(schema_name, table_list):
    connection = connector.Connection('landingdb')
    cursor = connection.cursor

    for table in table_list:
        cursor.execute(f'TRUNCATE TABLE {schema_name}.{table}')
        cursor.commit()