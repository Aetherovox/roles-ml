from datetime import datetime
import codecs


class ReedFile:

    def __init__(self, source_loc, filename):
        """

        :param source_loc: location to import reed file from, must include backslash at the end
        :param filename: reed filename to import
        """
        self.filename = filename
        self.source_loc = source_loc
        self.date = datetime.strptime(filename[14:22], '%Y%m%d')

    def contain_keyword(self, keyword):
        rf = codecs.open(self.source_loc + self.filename, encoding='utf8').read()

        kw_u = keyword.upper()
        # create keyword list which contains trailing/leading spaces, backslashes or forward slashes pairing
        kw_list = [" " + kw_u + " ",
                   " " + kw_u + ".",
                   " " + kw_u + "\\",
                   "\\" + kw_u + "\\",
                   "\\" + kw_u + " ",
                   "\\" + kw_u + ".",
                   " " + kw_u + "/",
                   "/" + kw_u + "/",
                   "/" + kw_u + " ",
                   "/" + kw_u + "."]

        return any(item in rf.upper() for item in kw_list)
