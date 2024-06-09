import json
from pathlib import Path

from conan import ConanFile
from conan.tools.layout import basic_layout
from conan.internal import check_duplicated_generator
from conan.tools.files import save


CONAN_COMMANDLINE_FILENAME = "conandeps.env"

class CommandlineDeps:
    def __init__(self, conanfile):
        """
        :param conanfile: ``< ConanFile object >`` The current recipe object. Always use ``self``.
        """
        self._conanfile = conanfile

    def generate(self) -> None:
        """
        Collects all dependencies and components, then, generating a Makefile
        """
        check_duplicated_generator(self, self._conanfile)

        host_req = self._conanfile.dependencies.host
        build_req = self._conanfile.dependencies.build  # tool_requires
        test_req = self._conanfile.dependencies.test

        content_buffer = ""

        # Filter the build_requires not activated for any requirement
        dependencies = [tup for tup in list(host_req.items()) + list(build_req.items()) + list(test_req.items()) if not tup[0].build]

        for require, dep in dependencies:
            # Require is not used at the moment, but its information could be used, and will be used in Conan 2.0
            if require.build:
                continue
            include_dir = Path(dep.package_folder) / 'include'
            package_dir = Path(dep.package_folder) / 'lib'
            content_buffer += json.dumps(dict(include_dirs=str(include_dir), library_dirs=str(package_dir)))

        save(self._conanfile, CONAN_COMMANDLINE_FILENAME, content_buffer)
        self._conanfile.output.info(f"Generated {CONAN_COMMANDLINE_FILENAME}")


class python_driverConan(ConanFile):
    win_bash = False

    settings = "os", "compiler", "build_type", "arch"
    requires = "libev/4.33"

    def layout(self):
        basic_layout(self)

    def generate(self):
        pc = CommandlineDeps(self)
        pc.generate()
