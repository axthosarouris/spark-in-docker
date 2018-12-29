from splittuple import *
from functools import reduce
from log4j import *

SPARK_EXECUTOR_OPTIONS = "spark.executor.extraJavaOptions"
CONF_FLAG = "--conf"


class ExecutorOptions:

    def __init__(self, log4j_path):
        self.log4j_path = log4j_path

    def append_java_executor_options(self, command, conflines):
        java_executor_options = self.__create_java_executor_options__(conflines)
        command.append("%s" % CONF_FLAG)
        command.append(java_executor_options)

    def __java_executor_option_for_log4j__(self):
        if self.log4j_path is not None:
            option_string = "%s=%s" % (
                SPARK_EXECUTOR_OPTIONS, log4j_configuration_option(self.log4j_path))
            return [SplitTuple(option_string)]
        else:
            return []

    def __create_java_executor_options__(self, conflines):
        java_options_lines = list(
            filter(lambda line: SPARK_EXECUTOR_OPTIONS in line, conflines))
        java_options_tuples = create_conf_tuples(java_options_lines)
        java_options_tuples = java_options_tuples + self.__java_executor_option_for_log4j__()
        merged_options_tuple = reduce(
            lambda tuple1, tuple2: tuple1.merge_value(tuple2),
            java_options_tuples)
        return merged_options_tuple.to_conf_line()


