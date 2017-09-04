from argparse import ArgumentParser
from leappto.driver.ssh import SSHConnectionError
from leappto.providers.local import LocalMachine
from leappto.providers.ssh import SSHMachine
from leappto.cli import start_agent_if_not_available
from subprocess import Popen, PIPE
import subprocess
import sys
from lightblue_client.client import ContainerCatalog
from system_blueprint.rpm_packages import RpmList


def _make_argument_parser():
    ap = ArgumentParser()
    ap.add_argument('machine', help='source machine to analyze')

    ap.add_argument('--identity', default=None,
                    help='Path to private SSH key for the source machine')
    ap.add_argument('--ask-pass', action='store_true',
                    help='Ask for SSH password for the source machine')
    ap.add_argument('--user', default=None,
                    help='Connect as this user to the source')

    return ap


def main():
    def _inspect_machine(host, shallow=True, user='vagrant'):
        try:
            if host in ('localhost', '127.0.0.1'):
                return LocalMachine(shallow_scan=shallow)
            return SSHMachine(host, user=user, shallow_scan=shallow)
        except SSHConnectionError as e:
            print("SSH error: {0}".format(e))
            return None
        except Exception as e:
            import traceback
            traceback.print_exc()
            return None

    def _set_ssh_config(username, identity):
        settings = {
            'StrictHostKeyChecking': 'no',
        }
        if username is not None:
            if not isinstance(username, str):
                raise TypeError("username should be str")
            settings['User'] = username
        if identity is not None:
            if not isinstance(identity, str):
                raise TypeError("identity should be str")
            settings['IdentityFile'] = identity

        ssh_options = ['-o {}={}'.format(k, v) for k, v in settings.items()]
        return ssh_options

    class AnalysisContext:
        def __init__(self, _machine, ssh_cfg):
            self.machine = _machine
            self.ssh_cfg = ssh_cfg

        @property
        def machine_addr(self):
            return machine.ip[0]

        def _ssh_base(self):
            return ['ssh'] + self.ssh_cfg + ['-4', self.machine_addr]

        def _ssh_make_child(self, cmd, **kwargs):
            ssh_cmd = self._ssh_base()
            ssh_cmd += [cmd]
            return Popen(ssh_cmd, **kwargs)

        def _ssh(self, cmd, **kwargs):
            return self._ssh_make_child(cmd, **kwargs).wait()

        def _ssh_sudo(self, cmd, **kwargs):
            sudo_cmd = "sudo bash -c '{}'".format(cmd)
            return self._ssh(sudo_cmd, **kwargs)

        def _ssh_out(self, cmd, **kwargs):
            child = self._ssh_make_child(cmd, stdout=PIPE, stderr=PIPE)
            output, err_output = child.communicate()
            if err_output:
                sys.stderr.write(err_output + b"\n")
            return child.returncode, output

        def _ssh_sudo_out(self, cmd, **kwargs):
            sudo_cmd = "sudo bash -c '{}'".format(cmd)
            return self._ssh_out(sudo_cmd, **kwargs)

        def ls(self):
            return self._ssh_sudo_out("ls -l")

        def rpm_qa(self):
            return self._ssh_sudo_out(
                "rpm -qa --queryformat \"%{NAME} %{VERSION} %{ARCH} "
                "%{NAME}-%{VERSION}-%{RELEASE}.%{ARCH}\n\"")

    ap = _make_argument_parser()
    parsed = ap.parse_args()

    if parsed.identity:
        start_agent_if_not_available()
        subprocess.call(['ssh-add', parsed.identity])

    source = parsed.machine
    print('Source machine: {}'.format(source))

    machine = _inspect_machine(source)
    if not machine:
        print("Source machine is not ready: " + source)
        sys.exit(-1)

    ac = AnalysisContext(machine, _set_ssh_config('vagrant', parsed.identity))

    rc, rpm_cmd = ac.rpm_qa()
    if rc:
        return rc
    src_rpm_list = RpmList()
    src_rpm_list.from_rpm_command(rpm_cmd)
    print("{} packages on the source system".format(len(src_rpm_list.list)))

    cc = ContainerCatalog()
    tagged_images = cc.get_images_with_openshift_tag("builder")
    print("{} potential images".format(len(tagged_images)))

    # Image gets 10 points for each package from source image that it contains
    fitness = {image: 0 for image in tagged_images}
    for image in tagged_images:
        for rpm in cc.get_image_rpms(image).list:
            if any(rpm.name == src_rpm.name for src_rpm in src_rpm_list.list):
                fitness[image] += 10

    best_image_id = sorted(fitness, key=fitness.get, reverse=True)[0]
    best_image = cc.get_image(best_image_id)
    print("Most suitable base image: {}".format(
        best_image['repositories'][0]['repository']))

if __name__ == '__main__':
    sys.exit(main())
