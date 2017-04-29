from canvas_data.api import CanvasDataAPI
import os
import pprint
pp = pprint.PrettyPrinter(indent=4)


try:
    API_KEY = os.environ['API_KEY']
    API_SECRET = os.environ['API_SECRET']
except KeyError:
    print "You must set both the API_KEY and API_SECRET environment variables."
    exit()

cd = CanvasDataAPI(api_key=API_KEY, api_secret=API_SECRET)

schema_versions = cd.get_schema_versions()

print 'Found {} schema versions.'.format(len(schema_versions))

schema = cd.get_schema('latest', key_on_tablenames=True)

pp.pprint(schema)

print 'The latest schema has {} tables.'.format(len(schema))

dumps = cd.get_dumps()

print 'There are a total of {} dumps available'.format(len(dumps))

one_dump = dumps[0]

dump_files = cd.get_file_urls(dump_id=one_dump['dumpId'])

dump_table_names = dump_files['artifactsByTable'].keys()

print 'The dump ID {} contains files for {} tables.'.format(one_dump['dumpId'], len(dump_table_names))

# are there tables that are in the schema but not in the dump, or vice-versa?
not_in_dump = [x for x in schema.keys() if x not in dump_table_names]

print 'These tables are present in the schema but missing from the dump: {}'.format(not_in_dump)

# files for a particular table
table_dump_files = cd.get_file_urls(table_name='course_dim')

# get all of the files for the course_dim table (this will take a while to run):
# cd.download_files(table_name='course_dim', directory='./downloads')

# get all of the files from the latest dump: (this will take a while to run)
# latest_dump_files = cd.download_files(dump_id='latest', include_requests=False, directory='./downloads')

# get just the course_dim table files from the latest dump
latest_course_files = cd.download_files(dump_id='latest', table_name='course_dim', directory='./downloads')
print 'Latest course files: {}'.format(latest_course_files)


# get a data file for a particular table
course_tsv = cd.get_data_for_table(table_name='course_dim')

print "got {}".format(course_tsv)

account_tsv = cd.get_data_for_table(table_name='account_dim')

print "got {}".format(account_tsv)

# this API takes a while to complete...
# sync_files = cd.get_sync_file_urls()
# pp.pprint(sync_files)



# more to come!
