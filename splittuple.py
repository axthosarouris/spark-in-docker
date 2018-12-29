
def create_conf_tuples(lineList):
  return list(map(lambda s: SplitTuple(s), lineList))

class SplitTuple:

  def __init__(self, input_string):
    self.inputString = input_string
    self.key = ""
    self.value = ""
    self._split_()

  def _splitIndex_(self):
    return self.inputString.find("=")

  def _split_(self):
    index = self._splitIndex_()
    self.key = self.inputString[0:index].strip()
    self.value = self.inputString[index + 1:].strip()

  def removeQuotes(self, input_string):
    if input_string.startswith("\""):
      return input_string[1:-1]
    else:
      return input_string

  def to_conf_line(self):
    result = "{0}={1}".format(self.key, self.value)
    return result

  def merge_value(self, split_tuple):
    if split_tuple.key != self.key:
      raise ValueError("Merging SplitTuples should have the same key")
    this_value = self.removeQuotes(self.value)
    that_value = self.removeQuotes(split_tuple.value)
    new_value = "\"%s %s\"" % (this_value, that_value)
    new_input_string = "%s=%s" % (self.key, new_value)
    return SplitTuple(new_input_string)
