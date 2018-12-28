#!/usr/bin/env python3

from splittuple import SplitTuple
import os
import subprocess
import sys
from functools import reduce

DLOG_J_CONFIGURATION = "-Dlog4j.configuration"

CONF_FLAG = "--conf"
FILES_FLAG = "--files"
DRIVER_JAVA_OPTIONS_CONF = "spark-driver-java-options.conf"
SUBMIT_PROPERTIES = "sparksubmit.conf"
SPARK_CONF = "spark-conf"
COMMENT = "#"
MIN_VALID_LENGTH = 3
LOG4J_FILE = "log4j.xml"
SPARK_DRIVER_OPTIONS = "--driver-java-options"
SPARK_EXECUTOR_OPTIONS = "spark.executor.extraJavaOptions"


def exists_log4j_file():
    files = os.listdir(SPARK_CONF)
    if LOG4J_FILE in files:
        return True
    else:
        return False


def log4j_abs_path():
    abs_path = os.path.join(SPARK_CONF, LOG4J_FILE)
    abs_path = os.path.abspath(abs_path)
    abs_path = "file://" + abs_path
    return abs_path


class Application:

    def __init__(self, mainclass=None):
        self.mainclass = mainclass
        self._update_log4j_path()

    def _file_lines_(self, file_name):
        conf_path = os.path.join("%s" % SPARK_CONF, "%s" % file_name)
        config_file = open(conf_path, "rt")
        lines = config_file.readlines()
        valid_lines = list(
            filter(lambda line: not line.startswith(COMMENT), lines))
        valid_lines = list(
            filter(lambda line: len(line) > MIN_VALID_LENGTH, valid_lines))
        return valid_lines

    def _conf_tuples_(self, lineList):
        return list(map(lambda s: SplitTuple(s), lineList))

    def _conf_lines_(self, file_name):
        lines = self._file_lines_(file_name)
        conf_tuples = self._conf_tuples_(lines)
        conflines = list(map(lambda s: s.to_conf_line(), conf_tuples))
        return conflines

    def _build_command_(self, mainclass, conflines, driver_java_lines):
        command = []
        command.append("spark-submit")
        command.append("--class")
        command.append(mainclass)
        command.append("--deploy-mode")
        command.append("client")
        command.append("--master")
        command.append("local[*]")

        self._append_submit_properties_(command, conflines)
        self._append_java_executor_options(command, conflines)
        self._append_java_driver_options(command, driver_java_lines)
        self._upload_log4j_file_(command)
        command.append(self.jarPath())
        return command

    def _append_submit_properties_(self, command, conflines):
        lines_without_java_options = list(
            filter(lambda line: SPARK_EXECUTOR_OPTIONS not in line, conflines))
        for line in lines_without_java_options:
            command.append("%s" % CONF_FLAG)
            command.append(line)

    def _append_java_driver_options(self, command, java_driver_options):
        options_string = ""
        if len(java_driver_options) > 0:
            options_string = " ".join(java_driver_options)
        if self.log4j_path is not None:
            options_string = " ".join(
                [options_string, self._log4j_configuration_flag_()])
        if len(options_string) > 0:
            options_string = "\"" + options_string.strip() + "\""
            command.append(SPARK_DRIVER_OPTIONS)
            command.append(options_string)
        return command

    def _create_java_executor_options(self, conflines):
        java_options_lines = list(
            filter(lambda line: SPARK_EXECUTOR_OPTIONS in line, conflines))
        java_options_tuples = self._conf_tuples_(java_options_lines)
        java_options_tuples = java_options_tuples + self._java_executor_option_for_log4j()
        merged_options_tuple = reduce(
            lambda tuple1, tuple2: tuple1.merge_value(tuple2),
            java_options_tuples)
        return merged_options_tuple.to_conf_line()

    def _java_executor_option_for_log4j(self):
        if self.log4j_path is not None:
            option_string = "%s=%s" % (
            SPARK_EXECUTOR_OPTIONS, self._log4j_configuration_flag_())
            return [SplitTuple(option_string)]
        else:
            return []

    def _append_java_executor_options(self, command, conflines):
        java_executor_options = self._create_java_executor_options(conflines)
        command.append("%s" % CONF_FLAG)
        command.append(java_executor_options)

    def _log4j_configuration_flag_(self):
        return "%s=%s" % (DLOG_J_CONFIGURATION, self.log4j_path)

    def _update_log4j_path(self):
        if exists_log4j_file():
            self.log4j_path = log4j_abs_path()
        else:
            self.log4j_path = None

    def _upload_log4j_file_(self, command):
        if self.log4j_path is not None:
            command.append(FILES_FLAG)
            command.append(self.log4j_path)



    def jarPath(self):
        return os.path.join("build", "libs", "wikidata-parser-fat.jar")

    def execute_command(self, command):
        subprocess.call(command)

    def run(self):
        conflines = self._conf_lines_(SUBMIT_PROPERTIES)
        driver_java_lines = self._conf_lines_("%s" % DRIVER_JAVA_OPTIONS_CONF)
        command = self._build_command_(self.mainclass, conflines,
                                       driver_java_lines)

        print(" ".join(command))
        self.execute_command(command)


def help():
    return  \
        """
        Usage: python run.py <mainClass>
                
        Spark initial configuration is stored in the file spark-conf/sparksubmit.conf
        
        """


def main():

    mainclass = None
    if (len(sys.argv) == 2):
        mainclass = sys.argv[1]
        application = Application(mainclass)
        application.run()
    else:
        print(help())



if __name__ == "__main__":
    main()
