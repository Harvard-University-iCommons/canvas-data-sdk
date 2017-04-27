from canvas_data.api import CanvasDataAPI
import os


try:
    API_KEY = os.environ['API_KEY']
    API_SECRET = os.environ['API_SECRET']
except KeyError:
    print "You must set both the API_KEY and API_SECRET environment variables."
    exit()

cd = CanvasDataAPI(api_key=API_KEY, api_secret=API_SECRET)

schema_versions = cd.get_schema_versions()

print 'Found {} schema versions.'.format(len(schema_versions))

schema = cd.get_schema('latest')

# some table names are incorrect in the schema; let's fix them
schema_table_names = [cd.fix_table_name(x) for x in schema['schema'].keys()]

print 'The latest schema is version {} and has {} tables.'.format(schema['version'], len(schema_table_names))

dumps = cd.get_dumps()

print 'There are a total of {} dumps available'.format(len(dumps))

one_dump = dumps[0]

dump_files = cd.get_file_urls(dump_id=one_dump['dumpId'])

dump_table_names = dump_files['artifactsByTable'].keys()

print 'The dump ID {} contains files for {} tables.'.format(one_dump['dumpId'], len(dump_table_names))

# are there tables that are in the schema but not in the dump, or vice-versa?
not_in_dump = [x for x in schema_table_names if x not in dump_table_names]
not_in_schema = [x for x in dump_table_names if x not in schema_table_names]

print 'These tables are present in the schema but missing from the dump: {}'.format(not_in_dump)
print 'These tables are present in the dump but missing from the schema: {}'.format(not_in_schema)

# more to come!
