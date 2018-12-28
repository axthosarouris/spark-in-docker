import os


def _file_lines_(file_name):
  conf_path = os.path.join("%s" % SPARK_CONF, "%s" % file_name)
  config_file = open(conf_path, "rt")
  lines = config_file.readlines()
  valid_lines = list(filter(lambda line: not line.startswith(COMMENT), lines))
  valid_lines = list(
    filter(lambda line: len(line) > MIN_VALID_LENGTH, valid_lines))
  return valid_lines
