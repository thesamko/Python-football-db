import json
string_with_json_obj = ""
json_data = string_with_json_obj.encode('utf8').decode('unicode_escape')
data = json.loads(json_data)
print()