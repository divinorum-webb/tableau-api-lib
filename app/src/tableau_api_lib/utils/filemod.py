import os
import shutil
import zipfile
from tableau_api_lib import TableauServerConnection
from tableau_api_lib.utils.common import get_server_netloc


def remap_xml_references(file, conn_source, conn_target) -> str:
    """
    Replaces any reference to the source server with a reference to the target server.
    :param str file: the file contents as a string of text
    :param TableauServerConnection conn_source: the source Tableau Server connection
    :param TableauServerConnection conn_target: the target Tableau Server connection
    :return: file contents as a string of text
    """
    file = file.replace("xml:base='{}'".format(conn_source.server),
                        "xml:base='{}'".format(conn_target.server))
    file = file.replace("xml:base=&apos;{}".format(conn_source.server),
                        "xml:base=&apos;{}".format(conn_target.server))
    file = file.replace("path='/t/{}/".format(conn_source.site_url),
                        "path='/t/{}/".format(conn_target.site_url))
    file = file.replace("path=&apos;/t/{}/".format(conn_source.site_url),
                        "path=&apos;/t/{}/".format(conn_target.site_url))
    file = file.replace("site='{}'".format(conn_source.site_url),
                        "site='{}'".format(conn_target.site_url))
    file = file.replace("site=&apos;{}".format(conn_source.site_url),
                        "site=&apos;{}".format(conn_target.site_url))
    file = file.replace("server='{}'".format(get_server_netloc(conn_source.server)),
                        "server='{}'".format(get_server_netloc(conn_target.server)))
    file = file.replace("server=&apos;{}".format(get_server_netloc(conn_source.server)),
                        "server=&apos;{}".format(get_server_netloc(conn_target.server)))
    return file


def copy_dir_to_zip(path, zip_file_obj) -> None:
    """
    Copies any extracted contents (excluding .tds or .twb) to the new zipfile object.
    :param str path: the path to the desired extracted files and directories
    :param zipfile.ZipFile zip_file_obj: the destination zipfile object
    :return: None
    """
    for root, dirs, files in os.walk(path):
        if str(root).__contains__('TwbxExternalCache'):
            pass
        else:
            for file in files:
                filename, file_extension = os.path.splitext(file)
                if file_extension not in ['.twb', '.tds']:
                    # zip_file_obj.write(os.path.join(root, file))
                    dir_to_write = os.path.split(root)[-1]
                    zip_file_obj.write(os.path.join(root, file), arcname=os.path.join(f'Data/{dir_to_write}', file))


def replace_unzipped_xml_file(file_path, conn_source, conn_target, extraction_dir_path) -> None:
    """
    Replace a .twb or .tds file, which was added as-is and not zipped.
    :param str file_path:
    :param TableauServerConnection conn_source: the source Tableau Server connection
    :param TableauServerConnection conn_target: the target Tableau Server connection
    :param str extraction_dir_path: path to the directory where the replacement file will be written
    :return: None
    """
    with open(file_path, 'r', encoding='utf-8') as original_file:
        file_contents = original_file.read()
        file_contents = remap_xml_references(file_contents, conn_source, conn_target)
    with open(extraction_dir_path + '/' + os.path.basename(file_path), 'w', encoding='utf-8') as new_file:
        new_file.write(file_contents)


def replace_zipped_xml_file(conn_source, conn_target, zip_file, tableau_file_base, extraction_dir_path) -> None:
    """
    Replace a .twbx or .tdsx file, which was downloaded as a zipped file.
    :param TableauServerConnection conn_source: the source Tableau Server connection
    :param TableauServerConnection conn_target: the target Tableau Server connection
    :param zipfile.ZipFile zip_file: the zipfile object
    :param str tableau_file_base: the Tableau file name
    :param str extraction_dir_path: path to the directory where the replacement file will be written
    :return: None
    """
    with zip_file.open(tableau_file_base, 'r') as file:
        file_contents = str(file.read(), 'utf-8')
        file_contents = remap_xml_references(file_contents, conn_source, conn_target)
    with open(extraction_dir_path + '/' + tableau_file_base, 'w', encoding='utf-8') as new_file:
        new_file.write(file_contents)


def generate_tableau_zipfile(extraction_dir_path, destination_dir_path, zip_file_name, xml_file_name) -> None:
    """
    Generate a Tableau .twbx or .tdsx file.
    :param str extraction_dir_path: path to the directory where the zipped file was extracted
    :param str destination_dir_path: path to the directory where the generated zipped file will be written
    :param str zip_file_name: the name of the zipped file
    :param str xml_file_name: the name of the .twb or .tds file within the zipped file
    :return: None
    """
    with zipfile.ZipFile(destination_dir_path + '/' + zip_file_name, 'w', zipfile.ZIP_DEFLATED) as modified_zipfile:
        modified_zipfile.write(extraction_dir_path + '/' + xml_file_name, xml_file_name)
        if 'Data' in os.listdir(extraction_dir_path):
            copy_dir_to_zip(path=extraction_dir_path, zip_file_obj=modified_zipfile)


def get_tableau_filenames(origin_path, extract_dir_path) -> (str, str):
    """
    Get the .twb or .tds file name from the directory where a zipped file was extracted.
    :param str origin_path: the origin path
    :param str extract_dir_path: the path to the directory where the zipped file was extracted.
    :return: str, str
    """
    origin_base = os.path.basename(origin_path)
    extracted_files = os.listdir(extract_dir_path)
    try:
        xml_file_base = [file for file in extracted_files if os.path.splitext(file)[1] in ['.tds', '.twb']].pop()
    except IndexError:
        raise Exception(f"No .tds or .twb files were discovered in the directory '{extract_dir_path}'.")
    return origin_base, xml_file_base


def modify_tableau_zipfile(zipfile_path, conn_source, conn_target, extraction_dir_path, destination_dir_path) -> None:
    """
    Modify a .twbx or .tdsx file and replace source connection references with target connection references.
    :param str zipfile_path: path to the zipped file that will be modified
    :param TableauServerConnection conn_source: the source Tableau Server connection
    :param TableauServerConnection conn_target: the target Tableau Server connection
    :param str extraction_dir_path: path to the directory where the zipped file has been extracted
    :param str destination_dir_path: path to the directory where the modified zipped file will be written
    :return: None
    """
    with zipfile.ZipFile(file=zipfile_path) as zip_file:
        print(f"extracting contents of file '{zipfile_path}'...")
        zip_file.extractall(path=extraction_dir_path)
        zip_file_base, xml_file_base = get_tableau_filenames(zipfile_path, extraction_dir_path)
        print(f"discovered a file to be modified: '{xml_file_base}'.")
        replace_zipped_xml_file(conn_source, conn_target, zip_file, xml_file_base, extraction_dir_path)
        print(f"successfully modified file '{xml_file_base}'.")
    generate_tableau_zipfile(extraction_dir_path, destination_dir_path, zip_file_base, xml_file_base)
    print(f"created the modified zipped file at '{extraction_dir_path}/{zip_file_base}'.")


def delete_temp_files(temp_dir_path=None) -> None:
    """
    Delete all directories and files nested within the specified directory path.
    :param str temp_dir_path: path to the directory whose nested contents will be deleted; the named directory persists
    :return: None
    """
    temp_dir_path = temp_dir_path or os.getcwd() + '/temp'
    print(f"deleting all nested content within dir '{temp_dir_path}'...")
    for root, dirs, files in os.walk(temp_dir_path):
        for file in files:
            print(f"removing file '{root}/{file}'")
            os.remove(os.path.join(root, file))
        for folder in dirs:
            print(f"removing dir '{root}/{folder}'")
            shutil.rmtree(os.path.join(root, folder))


def create_temp_dirs(temp_dir_path=None) -> None:
    temp_dir_path = temp_dir_path or os.getcwd() + '/temp'
    print(f"creating temp directory at '{temp_dir_path}'")
    try:
        os.makedirs(temp_dir_path, exist_ok=True)
        for content_type in ['datasources', 'workbooks', 'flows']:
            create_temp_dir_branches(temp_dir_path, content_type)
        print(f"creating dir '{temp_dir_path}/extracted'")
        os.makedirs(f"{temp_dir_path}/extracted")
    except FileExistsError:
        raise Exception(f"The directory '{temp_dir_path}' already exists and contains content.")


def create_temp_dir_branches(path, content_type):
    valid_content_types = ['datasources', 'workbooks', 'flows']
    if content_type not in valid_content_types:
        raise ValueError("Valid content types include 'datasources', 'workbooks', and 'flows'.")
    print(f"creating dir '{path}/{content_type}'")
    os.makedirs(f"{path}/{content_type}")
    print(f"creating dir '{path}/{content_type}/source'")
    os.makedirs(f"{path}/{content_type}/source")
    print(f"creating dir '{path}/{content_type}/target'")
    os.makedirs(f"{path}/{content_type}/target")


def set_temp_dirs(temp_dir):
    temp_dir = temp_dir or os.getcwd() + '/temp'
    extraction_dir = temp_dir + '/extracted'
    create_temp_dirs(temp_dir)
    return temp_dir, extraction_dir
