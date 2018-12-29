#!/usr/bin/env python3


import subprocess
import sys

from java_driver_options import *
from java_executor_options import *
from log4j import *

CONF_FLAG = "--conf"
FILES_FLAG = "--files"
DRIVER_JAVA_OPTIONS_CONF = "spark-driver-java-options.conf"
SUBMIT_PROPERTIES = "sparksubmit.conf"
SPARK_CONF_FOLDER = "conf"
COMMENT = "#"
MIN_VALID_LENGTH = 3


def set_log4j_filename():
    log4j_file = os.environ["LOG4J_FILENAME"]
    if log4j_file is not None:
        return log4j_file
    else:
        raise OSError("missing LOG4J_FILENAME env variable")


LOG4J_FILE = set_log4j_filename()


def file_lines(conf_file_path):
    if os.path.isfile(conf_file_path):
        config_file = open(conf_file_path, "rt")
        lines = config_file.readlines()
        valid_lines = list(
            filter(lambda line: not line.startswith(COMMENT), lines))
        valid_lines = list(
            filter(lambda line: len(line) > MIN_VALID_LENGTH, valid_lines))
        return valid_lines
    else:
        return list()


class Application:

    def __init__(self, jar_path=None, main_class=None):
        self.mainclass = main_class
        self.jarPath = jar_path
        self._update_log4j_path()

    def _conf_lines_(self, file_name):
        conf_file_path = os.path.join("%s" % SPARK_CONF_FOLDER, "%s" % file_name)
        if os.path.isfile(conf_file_path):
            lines = file_lines(conf_file_path)
            conf_tuples = create_conf_tuples(lines)
            conflines = list(map(lambda s: s.to_conf_line(), conf_tuples))
            return conflines
        raise OSError("Error reading file %s" % conf_file_path)

    def _build_command_(self, mainclass, conflines, driver_java_lines):
        command = []
        command.append("spark-submit")
        command.append("--class")
        command.append(mainclass)
        command.append("--deploy-mode")
        command.append("client")
        command.append("--master")
        command.append("local[*]")
        self.__append_submit_properties__(command, conflines)
        executor_options = ExecutorOptions(self.log4j_path)
        executor_options.append_java_executor_options(command, conflines)
        append_java_driver_options(command, driver_java_lines, self.log4j_path)
        self._upload_log4j_file_(command)
        command.append(self.jarPath)
        return command

    def __append_submit_properties__(self, command, conflines):
        lines_without_java_options = list(
            filter(lambda line: SPARK_EXECUTOR_OPTIONS not in line, conflines))
        for line in lines_without_java_options:
            command.append("%s" % CONF_FLAG)
            command.append(line)

    def _update_log4j_path(self):
        log4j_path = os.path.join(SPARK_CONF_FOLDER, LOG4J_FILE)
        if exists_log4j_file(log4j_path):
            self.log4j_path = log4j_abs_path(log4j_path)
        else:
            self.log4j_path = None

    def _upload_log4j_file_(self, command):
        if self.log4j_path is not None:
            command.append(FILES_FLAG)
            command.append(self.log4j_path)

    def execute_command(self, command):
        subprocess.call(command)

    def read_driver_java_lines(self, driver_java_options_conf_file):
        try:
            driver_java_lines = self._conf_lines_(DRIVER_JAVA_OPTIONS_CONF)
            return driver_java_lines
        except OSError:
            print("No file for java driver options found")
            print("Use %s file to include java driver options." % driver_java_options_conf_file)
            return list()

    def run(self):
        conflines = self._conf_lines_(SUBMIT_PROPERTIES)
        driver_java_lines = self.read_driver_java_lines(DRIVER_JAVA_OPTIONS_CONF)
        command = self._build_command_(self.mainclass, conflines,
                                       driver_java_lines)

        print(" ".join(command))
        self.execute_command(command)


def help():
    return \
        """
        Usage: python run.py <jarPath> <mainClass>
        Spark initial configuration is stored in the file conf/sparksubmit.conf
        
        """


def main():
    if (len(sys.argv) == 3):
        jarpath = sys.argv[1]
        mainclass = sys.argv[2]
        application = Application(jarpath, mainclass)
        application.run()
    else:
        print(help())


if __name__ == "__main__":
    main()
