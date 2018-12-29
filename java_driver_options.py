from log4j import *

SPARK_DRIVER_OPTIONS = "--driver-java-options"


def append_java_driver_options(command, java_driver_options, log4j_path):
    options_string = ""
    if len(java_driver_options) > 0:
        options_string = " ".join(java_driver_options)
    options_string = add_log4j_path_to_driver_options(log4j_path, options_string)
    if len(options_string) > 0:
        options_string = "\"" + options_string.strip() + "\""
        command.append(SPARK_DRIVER_OPTIONS)
        command.append(options_string)
    return command


def add_log4j_path_to_driver_options(log4j_path, options_string):
    result_str = options_string
    if log4j_path is not None:
        result_str = " ".join(
            [options_string, log4j_configuration_option(log4j_path)])
    return result_str
