import os

DLOG_J_CONFIGURATION = "-Dlog4j.configuration"
ABS_FILE_URL = "file://"


def log4j_configuration_option(log4j_path):
    return "%s=%s" % (DLOG_J_CONFIGURATION, log4j_path)


def exists_log4j_file(log4j_path):
    return os.path.isfile(log4j_path)


def log4j_abs_path(log4j_path):
    abs_path = os.path.abspath(log4j_path)
    if not abs_path.startswith(ABS_FILE_URL):
        abs_path = ABS_FILE_URL + abs_path
    return abs_path
