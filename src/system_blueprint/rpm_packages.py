class RpmPkg:
    def __init__(self, pkg_object):
        self.name = pkg_object["name"]
        self.version = pkg_object["version"]
        self.architecture = pkg_object["architecture"]
        self.nvra = pkg_object["nvra"]


class RpmList:
    def __init__(self):
        self.list = set()

    def add(self, pkg):
        self.list.add(RpmPkg(pkg))

    def from_rpm_command(self, rpm_result):
        for rpm in rpm_result.split("\n"):
            if not rpm: continue
            self.add(dict(zip(["name", "version", "architecture", "nvra"],
                              rpm.split())))
