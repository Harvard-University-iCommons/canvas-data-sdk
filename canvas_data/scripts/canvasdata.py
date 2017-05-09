import yaml
import json

import click
from canvas_data.api import CanvasDataAPI


@click.group()
@click.option('-c', '--config', type=click.File('r'), envvar='CANVAS_DATA_CONFIG')
@click.option('--api-key', envvar='CANVAS_DATA_API_KEY')
@click.option('--api-secret', envvar='CANVAS_DATA_API_SECRET')
@click.pass_context
def cli(ctx, config, api_key, api_secret):
    """A command-line tool to work with Canvas Data. Command-specific help
    is available at: canvas-data COMMAND --help"""
    # if a config file was specified, read settings from that
    if config:
        ctx.obj = yaml.load(config)
    else:
        ctx.obj = {}

    # if options were passed in, use them, possibly overriding config file settings
    if api_key:
        ctx.obj['api_key'] = api_key
    if api_secret:
        ctx.obj['api_secret'] = api_secret


@cli.command()
@click.option('--version', default='latest')
@click.pass_context
def get_schema(ctx, version):
    """Gets a particular version of the Canvas Data schema (latest by default) and outputs as JSON"""
    cd = CanvasDataAPI(
        api_key=ctx.obj.get('api_key'),
        api_secret=ctx.obj.get('api_secret')
    )

    schema = cd.get_schema(version, key_on_tablenames=True)
    click.echo(json.dumps(schema, sort_keys=True, indent=4))


@cli.command()
@click.option('--dump-id', default='latest', help='get files for this dump (defaults to the latest dump)')
@click.option('--download-dir', default=None, type=click.Path(), help='store downloaded files in this directory')
@click.option('--table', default=None, help='(optional) only get the files for a particular table')
@click.option('--force', is_flag=True, default=False, help='re-download files even if they already exist (default False)')
@click.pass_context
def get_dump_files(ctx, dump_id, download_dir, table, force):
    """Downloads the Canvas Data files for a particular dump. Can be optionally limited to a single table."""
    if download_dir:
        ctx.obj['download_dir'] = download_dir
    if table:
        ctx.obj['table'] = table
    cd = CanvasDataAPI(
        api_key=ctx.obj.get('api_key'),
        api_secret=ctx.obj.get('api_secret')
    )

    # first, get the dump details so we can extract the list of fragment files to download
    dump_files = []
    dump_details = cd.get_file_urls(dump_id=dump_id)
    if ctx.obj.get('table'):
        dump_files.extend(dump_details['artifactsByTable'][ctx.obj['table']]['files'])
    else:
        for k, v in dump_details['artifactsByTable'].iteritems():
            if k == 'requests':
                continue
            dump_files.extend(v['files'])

    filenames = []
    progress_label = '{: <23}'.format('Downloading {} files'.format(len(dump_files)))
    with click.progressbar(dump_files, label=progress_label) as file_list:
        for f in file_list:
            filenames.append(cd.get_file(file=f, download_directory=ctx.obj['download_dir'], force=force))


@cli.command()
@click.option('--dump-id', default='latest', help='get files for this dump (defaults to the latest dump)')
@click.option('--download-dir', default=None, type=click.Path(), help='store downloaded files in this directory')
@click.option('--data-dir', default=None, type=click.Path(), help='store unpacked files in this directory')
@click.option('--table', default=None, help='(optional) only get the files for a particular table')
@click.option('--force', is_flag=True, default=False, help='re-download/re-unpack files even if they already exist (default False)')
@click.pass_context
def unpack_dump_files(ctx, dump_id, download_dir, data_dir, table, force):
    """
    Downloads, uncompresses and re-assembles the Canvas Data files for a dump. Can be
    optionally limited to a single table.
    """
    if download_dir:
        ctx.obj['download_dir'] = download_dir
    if data_dir:
        ctx.obj['data_dir'] = data_dir
    if table:
        ctx.obj['table'] = table
    cd = CanvasDataAPI(
        api_key=ctx.obj.get('api_key'),
        api_secret=ctx.obj.get('api_secret')
    )
    # first make sure all of the files are downloaded
    ctx.invoke(get_dump_files, dump_id=dump_id, download_dir=ctx.obj['download_dir'], table=ctx.obj.get('table'), force=force)

    table_names = []
    if ctx.obj.get('table'):
        table_names.append(ctx.obj['table'])
    else:
        dump_details = cd.get_file_urls(dump_id=dump_id)
        table_names.extend(dump_details['artifactsByTable'].keys())
        table_names.remove('requests')

    data_file_names = []
    progress_label = '{: <23}'.format('Unpacking {} tables'.format(len(table_names)))
    with click.progressbar(table_names, label=progress_label) as tnames:
        for t in tnames:
            data_file_names.append(cd.get_data_for_table(table_name=t,
                                                         dump_id=dump_id,
                                                         download_directory=ctx.obj['download_dir'],
                                                         data_directory=ctx.obj['data_dir'],
                                                         force=force))