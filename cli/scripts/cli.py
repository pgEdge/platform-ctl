
#  Copyright 2022-2024 PGEDGE  All rights reserved. #

import sys, os

if sys.version_info < (3, 9):
    maj = sys.version_info.major
    min = sys.version_info.minor
    print(f"ERROR: Python 3.9 is minimally required (found Python {maj}.{min})")
    sys.exit(1)

IS_64BITS = sys.maxsize > 2**32
if not IS_64BITS:
    print("ERROR: This is a 32bit machine and we are 64bit.")
    sys.exit(1)

if os.path.isdir("conf"):
    print("WARNING: moving 'conf' dir to `data/conf`")
    os.system(f"mv -f conf  data/.")
    os.system("rm -f ctl nc nodectl")

MY_HOME = os.getenv("MY_HOME", None)
MY_CMD = os.getenv("MY_CMD", None)
MY_LITE = os.getenv("MY_LITE", None)
if not (MY_HOME and MY_CMD and MY_LITE):
    print("Required Envs not set (MY_HOME, MY_CMD, MY_LITE)")
    sys.exit(1)

import time, datetime, platform, tarfile, sqlite3
import json, glob, re, io, traceback, logging, logging.handlers
from shutil import copy2
from semantic_version import Version


# Our own library files ##########################################
sys.path.append(os.path.join(os.path.dirname(__file__), "lib"))

this_platform_system = str(platform.system())
platform_lib_path = os.path.join(os.path.dirname(__file__), "lib", this_platform_system)

if os.path.exists(platform_lib_path):
    if platform_lib_path not in sys.path:
        sys.path.append(platform_lib_path)

import util, api, startup, meta
my_logger=util.my_logger

my_conf = os.path.join(util.MY_HOME, "data", "conf")
if not util.is_writable(my_conf):
    rc = os.system(f"sudo mkdir -p {my_conf}")
    if rc == 0:
        pass
    else:
        print(f"Unable to write to '{my_conf}' directory")
        sys.exit(1)

if util.get_value("GLOBAL", "PLATFORM", "") in ("", "posix", "windoze"):
    util.set_value("GLOBAL", "PLATFORM", util.get_default_pf())

ansi_escape = re.compile(r"\x1b[^m]*m")

dep9 = util.get_depend()

fire_list = [
    "service",
    "um",
    "spock",
    "cluster",
    "ace",
    "cloud",
    "db",
    "app",
    "setup",
    "upgrade-cli"
]

py3_check_list  = ["info", "spock", "cluster", "localhost"]

fire_contrib = ["localhost"]

native_list = ["backrest", "ansible", "patroni", "etcd", "bouncer"]

mode_list_advanced = [
    "kill",
    "config",
    "init",
    "clean",
    "download",
    "useradd",
    "spock",
    "pgbin",
    "psql",
    "pg_isready",
    "cluster",
    "ace",
    "enable",
    "upgrade",
    "cloud",
    "db",
    "app",
    "update",
    "disable",
    "tune",
    "get",
    "top",
    "set",
    "unset",
    "reload",
]

mode_list = (
    [
        "start",
        "stop",
        "restart",
        "status",
        "list",
        "info",
        "help",
        "install",
        "remove",
        "--pg",
        "--start",
        "--disabled",
        "--no-restart",
        "--no-preload",
        "--help",
        "--json",
        "--jsonp",
        "--svcs",
        "--list",
        "-y",
        "-t",
        "--pause",
        "--verbose",
        "-v",
        "--debug",
        "--debug2",
    ]
    + fire_list
    + fire_contrib
    + native_list
    + mode_list_advanced
)

mode_list

ignore_comp_list = (
    [
        "get",
        "set",
        "unset",
        "pgbin",
        "psql",
        "pg_isready",
        "service",
        "useradd",
        "change-pgconf",
    ]
    + fire_list
    + fire_contrib
    + native_list
)

no_log_commands = ["status", "info", "list", "top", "get", "metrics-check"]

lock_commands = (
    ["install", "remove", "update", "upgrade", "service"]
    + fire_list
    + fire_contrib
    + native_list
)

my_depend = []
installed_comp_list = []
global check_sum_match
check_sum_match = True

backup_dir = os.path.join(util.MY_HOME, "data", "conf", "backup")
backup_target_dir = os.path.join(backup_dir, time.strftime("%Y%m%d%H%M"))

pid_file = os.path.join(util.MY_HOME, "data", "conf", "cli.pid")

isJSON = util.isJSON


def fire_away(p_mode, p_args):
    util.message(f"cli.fire_away({p_mode}, {p_args})", "debug")

    mode = p_mode
    if p_mode == "upgrade-cli":
        mode = "upgrade-cli-fire"

    py_file = f"{mode}.py"
    py3 = sys.executable
    if os.path.exists(py_file):
        cmd = f"{py3} {py_file}"
    else:
        if p_mode in fire_contrib:
            cmd = f"{py3} hub/scripts/contrib/{py_file}"
        else:
            cmd = f"{py3} hub/scripts/{py_file}"

    for n in range(2, len(p_args)):
        parm = p_args[n]
        cmd = cmd + ' "' + parm + '"'
    rc = os.system(cmd)
    if rc == 0:
        sys.exit(0)

    sys.exit(1)


def get_next_arg(p_arg):
    i = 0
    next_arg = ""
    while i < len(args):
        arg = args[i]
        if arg == p_arg:
            if i < (len(args) - 1):
                next_arg = args[i + 1]
                break
        i += 1

    return next_arg


# Get Dependency List #########################################
def get_depend_list(p_list, p_display=True):
    if p_list == ["all"]:
        if p_mode in ("install"):
            pp_list = available_comp_list
        else:
            pp_list = installed_comp_list
    else:
        pp_list = p_list
    ndx = 0
    deplist = []
    for c in pp_list:
        ndx = ndx + 1
        new_list = list_depend_recur(c)
        deplist.append(c)
        for c1 in new_list:
            deplist.append(c1)

    deplist = set(deplist)

    num_deplist = []
    ndx = 0
    for ndx, comp in enumerate(deplist):
        num_deplist.append(get_comp_num(comp) + ":" + str(comp))

    sorted_depend_list = []
    for c in sorted(num_deplist):
        comp = str(c[4:])
        sorted_depend_list.append(c[4:])

    msg = "  " + str(sorted_depend_list)
    my_logger.info(msg)
    if isJSON:
        dictDeplist = {}
        dictDeplist["state"] = "deplist"
        dictDeplist["component"] = p_list
        dictDeplist["deps"] = sorted_depend_list
        msg = json.dumps([dictDeplist])
    if p_display:
        if not isSILENT:
            print(msg)
    return sorted_depend_list


# Check if component is already downloaded
def is_downloaded(p_comp, component_name=None):
    conf_cache = "data" + os.sep + "conf" + os.sep + "cache"
    zip_file = p_comp + ".tgz"
    checksum_file = zip_file + ".sha512"

    if os.path.isfile(conf_cache + os.sep + checksum_file):
        if util.validate_checksum(
            conf_cache + os.sep + zip_file, conf_cache + os.sep + checksum_file
        ):
            return True

    msg = ""
    if not util.http_get_file(
        isJSON, checksum_file, REPO, conf_cache, False, msg, component_name
    ):
        return False

    return util.validate_checksum(
        conf_cache + os.sep + zip_file, conf_cache + os.sep + checksum_file
    )


# Get Component Number ####################################################
def get_comp_num(p_app):
    ndx = 0
    for comp in dep9:
        ndx = ndx + 1
        if comp[0] == p_app:
            if ndx < 10:
                return "00" + str(ndx)
            elif ndx < 100:
                return "0" + str(ndx)

            return str(ndx)
    return "000"


class ProgressTarExtract(io.FileIO):
    component_name = ""
    file_name = ""

    def __init__(self, path, *args, **kwargs):
        self._total_size = os.path.getsize(path)
        io.FileIO.__init__(self, path, *args, **kwargs)

    def read(self, size):
        if not os.path.isfile(pid_file):
            raise KeyboardInterrupt("No lock file exists.")
        percentage = self.tell() * 100 / self._total_size
        return io.FileIO.read(self, size)


# Install Component ######################################################
def install_comp(p_app, p_ver=0, p_rver=None, p_re_install=False):
    util.message(f"cli.install_comp(p_app={p_app}, p_ver={p_ver}, p_rver={p_rver}, p_re_install={p_re_install})", "debug")
    if p_ver is None:
        p_ver = 0
    if p_rver:
        parent = util.get_parent_component(p_app, p_rver)
    else:
        parent = util.get_parent_component(p_app, p_ver)

    if parent != "":
        parent_state = util.get_comp_state(parent)
        if parent_state == "NotInstalled":
            errmsg = "{0} has to be installed before installing {1}".format(
                parent, p_app
            )
            if isJSON:
                json_dict = {}
                json_dict["state"] = "error"
                json_dict["msg"] = errmsg
                errmsg = json.dumps([json_dict])
            print(errmsg)
            exit_cleanly(1)

    state = util.get_comp_state(p_app)
    
    if state in ["Enabled", "Disabled"]:
        util.message(f"{p_app} is already installed & {state}", "warn")
        return("enable")

    if state == "NotInstalled" or p_re_install:
        if p_ver == 0:
            ver = meta.get_latest_ver_plat(p_app)
            if ver == "-1":
                util.exit_message(
                    f"{p_app} not available on {util.get_pf()} platform", 1
                )
        else:
            ver = p_ver

        print("")
        if meta.check_pre_reqs(p_app, ver):
            pass
        else:
            exit_cleanly(1)

        base_name = p_app + "-" + ver
        conf_cache = "data" + os.sep + "conf" + os.sep + "cache"
        file = base_name + ".tgz"
        zip_file = conf_cache + os.sep + file
        json_dict = {}
        json_dict["component"] = p_app
        json_dict["file"] = file
        if isJSON:
            json_dict["state"] = "download"
            json_dict["status"] = "start"
            print(json.dumps([json_dict]))

        if os.path.exists(zip_file) and is_downloaded(base_name, p_app):
            msg = "File is already downloaded."
            my_logger.info(msg)
            if isJSON:
                json_dict["status"] = "complete"
                msg = json.dumps([json_dict])
            if not isSILENT:
                print(msg)
        elif not retrieve_comp(base_name, p_app):
            exit_cleanly(1)

        if not isSILENT:
            util.message("\nUnpacking " + file)
        full_file = conf_cache + os.sep + file

        if platform.system() in ("Linux", "Darwin"):
            return util.posix_unpack(full_file)

        tarFileObj = ProgressTarExtract(full_file)
        tarFileObj.component_name = p_app
        tarFileObj.file_name = file

        tar = tarfile.open(fileobj=tarFileObj, mode="r")

        try:
            tar.extractall(path=".")
        except KeyboardInterrupt:
            temp_tar_dir = os.path.join(MY_HOME, p_app)
            util.delete_dir(temp_tar_dir)
            msg = "Unpacking cancelled for file %s" % file
            my_logger.error(msg)
            return_code = 1
            if isJSON:
                json_dict = {}
                json_dict["state"] = "unpack"
                json_dict["status"] = "cancelled"
                json_dict["component"] = p_app
                json_dict["msg"] = msg
                msg = json.dumps([json_dict])
                return_code = 0
            util.exit_message(msg, return_code)
        except Exception as e:
            temp_tar_dir = os.path.join(MY_HOME, p_app)
            util.delete_dir(temp_tar_dir)
            util.message("Unpacking failed for file %s" % str(e), "error")
            my_logger.error(traceback.format_exc())
            return_code = 1
            if isJSON:
                json_dict = {}
                json_dict["state"] = "error"
                json_dict["component"] = p_app
                json_dict["msg"] = str(e)
                msg = json.dumps([json_dict])
                return_code = 0
            util.exit_message(msg, return_code)

        tar.close
        if isJSON:
            util.message("Unpack complete")


def upgrade_component(p_comp):
    present_version = meta.get_version(p_comp)
    if not present_version:
        return
    present_state = util.get_comp_state(p_comp)
    server_port = util.get_comp_port(p_comp)
    try:
        c = connL.cursor()

        sql = (
            "SELECT version, platform FROM versions "
            + " WHERE component = '"
            + p_comp
            + "' \n"
            + "   AND "
            + util.like_pf("platform")
            + " \n"
            + "   AND is_current = 1"
        )
        c.execute(sql)
        row = c.fetchone()
        c.close()
    except Exception as e:
        fatal_sql_error(e, sql, "upgrade_component()")

    if str(row) == "None":
        return

    update_version = str(row[0])
    platform = str(row[1])
    if platform > "":
        platform = util.get_pf()

    is_update_available = 0
    cv = Version.coerce(update_version)
    iv = Version.coerce(present_version)
    if cv > iv:
        is_update_available = 1

    if is_update_available == 0:
        return 1

    if present_state == "NotInstalled":
        meta.update_component_version(p_comp, update_version)
        return 0

    server_running = False
    if server_port > "1":
        server_running = util.is_socket_busy(int(server_port), p_comp)

    if server_running:
        util.run_script(p_comp, "stop-" + p_comp, "stop")

    msg = (f"upgrading {p_comp} from ({present_version} to ({update_version})")

    my_logger.info(msg)
    if isJSON:
        print(
            '[{"state":"update","status":"start","component":"'
            + p_comp
            + '","msg":"'
            + msg
            + '"}]'
        )
    else:
        if not isSILENT:
            print(msg)

    components_stopped = []
    dependent_components = meta.get_dependent_components(p_comp)
    isExt = meta.is_extension(p_comp)
    if isExt:
        parent = util.get_parent_component(p_comp, 0)
        dependent_components.append([parent])

    for dc in dependent_components:
        d_comp = str(dc[0])
        d_comp_present_state = util.get_comp_state(d_comp)
        d_comp_server_port = util.get_comp_port(d_comp)
        d_comp_server_running = False
        if d_comp_server_port > "1":
            d_comp_server_running = util.is_socket_busy(
                int(d_comp_server_port), p_comp
            )
        if d_comp_server_running:
            my_logger.info("Stopping the " + d_comp + " to upgrade the " + p_comp)
            util.run_script(d_comp, "stop-" + d_comp, "stop")
            components_stopped.append(d_comp)

    rc = unpack_comp(p_comp, present_version, update_version)

    os.environ[p_comp + "_present_version"] = present_version
    os.environ[p_comp + "_update_version"] = update_version
    if rc == 0:
        meta.update_component_version(p_comp, update_version)
        util.run_script(p_comp, "update-" + p_comp, "update")
        if isJSON:
            msg = (
                "updated "
                + p_comp
                + " from ("
                + present_version
                + ") to ("
                + update_version
                + ")"
            )
            print(
                '[{"status": "complete", "state": "update", "component": "'
                + p_comp
                + '","msg":"'
                + msg
                + '"}]'
            )

    if server_running:
        util.run_script(p_comp, "start-" + p_comp, "start")

    for dc in components_stopped:
        my_logger.info("Starting the " + dc + " after upgrading the " + p_comp)
        util.run_script(dc, "start-" + dc, "start")

    return 0


def unpack_comp(p_app, p_old_ver, p_new_ver):
    state = util.get_comp_state(p_app)

    base_name = p_app + "-" + meta.get_latest_ver_plat(p_app, p_new_ver)

    file = base_name + ".tgz"
    zip_file = os.path.join(MY_HOME, "conf", "cache", file)

    if os.path.exists(zip_file) and is_downloaded(base_name, p_app):
        msg = "File is already downloaded."
        my_logger.info(msg)
        if isJSON:
            json_dict = {}
            json_dict["state"] = "download"
            json_dict["component"] = p_app
            json_dict["status"] = "complete"
            json_dict["file"] = file
            msg = json.dumps([json_dict])
        print(msg)
    elif not retrieve_comp(base_name, p_app):
        return 1

    msg = " Unpacking " + p_app + "(" + p_new_ver + ") over (" + p_old_ver + ")"
    my_logger.info(msg)

    file = base_name + ".tgz"

    if isJSON:
        print(
            '[{"state":"unpack","status":"start","component":"'
            + p_app
            + '","msg":"'
            + msg
            + '","file":"'
            + file
            + '"}]'
        )
    else:
        if not isSILENT:
            print(msg)

    return_value = 0

    tarFileObj = ProgressTarExtract("data" + os.sep + "conf" + os.sep + "cache" + os.sep + file)
    tarFileObj.component_name = p_app
    tarFileObj.file_name = file
    tar = tarfile.open(fileobj=tarFileObj, mode="r")

    new_comp_dir = p_app + "_new"
    old_comp_dir = p_app + "_old"

    try:
        tar.extractall(path=new_comp_dir)
    except KeyboardInterrupt as e:
        util.delete_dir(new_comp_dir)
        msg = "Unpacking cancelled for file %s" % file
        if isJSON:
            json_dict = {}
            json_dict["state"] = "unpack"
            json_dict["status"] = "cancelled"
            json_dict["component"] = p_app
            json_dict["msg"] = msg
            msg = json.dumps([json_dict])
        if not isSILENT:
            print(msg)
        my_logger.error(msg)
        return 1
    except Exception as e:
        util.delete_dir(new_comp_dir)
        msg = "Unpacking failed for file %s" % str(e)
        if isJSON:
            json_dict = {}
            json_dict["state"] = "error"
            json_dict["component"] = p_app
            json_dict["msg"] = str(e)
            msg = json.dumps([json_dict])
        if not isSILENT:
            print(msg)
        my_logger.error(msg)
        my_logger.error(traceback.format_exc())
        return 1

    tar.close

    isExt = meta.is_extension(p_app)
    if isExt:
        try:
            parent = util.get_parent_component(p_app, 0)
            backup_parent = os.path.join(backup_target_dir, parent)
            os.system(f"mkdir -p {backup_parent}")
            my_logger.info("backing up the parent component %s " % parent)
            util.copytree(f"{os.path.join(MY_HOME, parent)}  {backup_parent}")

            manifest_file_name = p_app + ".manifest"
            manifest_file_path = os.path.join(MY_HOME, "data", "conf", manifest_file_name)
            try:
                os.remove(manifest_file_path)
            except Exception:
                pass

            my_logger.info("copying new extension files : " + manifest_file_name)
            util.copy_extension_files(p_app, parent, upgrade=True)
        except Exception as e:
            error_msg = "Error while upgrading the " + p_app + " : " + str(e)
            my_logger.error(error_msg)
            my_logger.error(traceback.format_exc())
            if isJSON:
                json_dict = {}
                json_dict["state"] = "error"
                json_dict["component"] = p_app
                json_dict["msg"] = str(e)
                error_msg = json.dumps([json_dict])
            if not isSILENT:
                print(error_msg)
            return_value = 1
    else:
        try:
            if not os.path.exists(backup_dir):
                os.mkdir(backup_dir)
            if not os.path.exists(backup_target_dir):
                os.mkdir(backup_target_dir)
            if not os.path.exists(os.path.join(backup_target_dir, p_app)):
                my_logger.info("backing up the old version of %s " % p_app)
                util.copytree(f"{os.path.join(MY_HOME, p_app)}  {os.path.join(backup_target_dir, p_app)}")

            msg = p_app + " upgrade staged for completion."
            my_logger.info(msg)
            if p_app in ('hub'):
                copy2(os.path.join(MY_HOME, MY_CMD), backup_target_dir)
                os.rename(new_comp_dir, "hub_new")
                ## run the update_hub script in the _new directory
                upd_hub_cmd = sys.executable + " hub_new" + os.sep + "hub" + os.sep + "scripts" + os.sep + "update_hub.py "
                os.system(upd_hub_cmd + p_old_ver + " " + p_new_ver)
            else:
                my_logger.info("renaming the existing folder %s" % p_app)
                os.rename(p_app, p_app+"_old")
                my_logger.info("copying the new files to folder %s" % p_app)

                util.copytree(f"{os.path.join(MY_HOME, new_comp_dir, p_app)}  {os.path.join(MY_HOME, p_app)}")

                my_logger.info("Restoring the conf and extension files if any")
                util.restore_conf_ext_files(os.path.join(MY_HOME, p_app+"_old"), os.path.join(MY_HOME, p_app))
                my_logger.info(p_app + " upgrade completed.")
        except Exception as upgrade_exception:
            error_msg = "Error while upgrading the " + p_app + " : " + str(upgrade_exception)
            my_logger.error(error_msg)
            my_logger.error(traceback.format_exc())
            if isJSON:
                json_dict = {}
                json_dict['state'] = "error"
                json_dict['component'] = p_app
                json_dict['msg'] = str(upgrade_exception)
                error_msg = json.dumps([json_dict])
            if not isSILENT:
                print(error_msg)
                return_value = 1

    if os.path.exists(os.path.join(MY_HOME, new_comp_dir)):
        util.delete_dir(os.path.join(MY_HOME, new_comp_dir))
    if os.path.exists(os.path.join(MY_HOME, old_comp_dir)):
        util.delete_dir(os.path.join(MY_HOME, old_comp_dir))

    return return_value


def list_depend_recur(p_app):
    for i in dep9:
        if i[0] == p_app:
            if i[1] not in my_depend and meta.is_dependent_platform(i[1]):
                my_depend.append(i[1])
                list_depend_recur(i[1])
    return my_depend


def update_component_state(p_app, p_mode, p_ver=None):
    util.message(f"cli.update_component_state(p_app={p_app}, p_mode={p_mode}, p_ver={p_ver}", "debug")
    new_state = "Disabled"
    if p_mode == "enable":
        new_state = "Enabled"
    elif p_mode == "install":
        new_state = "Enabled"
    elif p_mode == "remove":
        new_state = "NotInstalled"

    current_state = util.get_comp_state(p_app)
    ver = ""

    if current_state == new_state:
        return

    if p_mode == "disable" or p_mode == "remove":
        util.run_script(p_app, "stop-" + p_app, "kill")

    try:
        c = connL.cursor()

        if p_mode in ("enable", "disable"):
            ver = meta.get_version(p_app)
            sql = "UPDATE components SET status = ? WHERE component = ?"
            c.execute(sql, [new_state, p_app])

        if p_mode == "remove":
            ver = meta.get_version(p_app)
            sql = "DELETE FROM components WHERE component = ?"
            c.execute(sql, [p_app])

        if p_mode == "install":
            sql = (
                "INSERT INTO components (component, project, version, platform, port, status) "
                + "SELECT v.component, r.project, v.version, "
                + " CASE WHEN v.platform='' THEN '' ELSE '"
                + util.get_pf()
                + "' END, p.port, 'Enabled' "
                + "  FROM versions v, releases r, projects p "
                + " WHERE v.component = ? "
                + "   AND v.component = r.component "
                + "   AND r.project = p.project "
                + "   AND v.version = ? "
            )
            if p_ver:
                ver = p_ver
            else:
                ver = meta.get_current_version(p_app)
            c.execute(sql, [p_app, ver])

        connL.commit()
        c.close()
    except Exception as e:
        fatal_sql_error(e, sql, "update_component_state()")

    msg = p_app + " " + new_state
    my_logger.info(msg)
    if isJSON:
        msg = '[{"status":"wip","msg":"' + msg + '"}]'

    return


## Download tarball component and verify against checksum ###############
def retrieve_comp(p_base_name, component_name=None):
    conf_cache = "data" + os.sep + "conf" + os.sep + "cache"
    zip_file = p_base_name + ".tgz"
    checksum_file = zip_file + ".sha512"
    global download_count
    download_count += 1

    msg = "Get:" + str(download_count) + " " + REPO + " " + p_base_name
    my_logger.info(msg)
    display_status = True
    if isSILENT:
        display_status = False
    if not util.http_get_file(
        isJSON, zip_file, REPO, conf_cache, display_status, msg, component_name
    ):
        return False

    msg = "Preparing to unpack " + p_base_name
    if not util.http_get_file(
        isJSON, checksum_file, REPO, conf_cache, False, msg, component_name
    ):
        return False

    return util.validate_checksum(
        conf_cache + os.sep + zip_file, conf_cache + os.sep + checksum_file
    )


def get_comp_display():
    comp_display = "("
    for comp in installed_comp_list:
        if not comp_display == "(":
            comp_display += ", "
        comp_display += comp
    comp_display += ")"
    return comp_display


def get_mode_display():
    mode_display = "("
    for mode in mode_list:
        if not mode_display == "(":
            mode_display += ", "
        mode_display += mode
    mode_display += ")"
    return mode_display


def get_help_text():
    helpf = "README.md"
    helpfile = os.path.dirname(os.path.realpath(__file__)) + "/../doc/" + helpf
    s = util.read_file_string(helpfile, "quiet")

    lines = s.split("\n")
    new_s = ""
    for line in lines:
        fmtd_line = api.format_help(line)
        if not fmtd_line == None:
            new_s = new_s + api.format_help(line) + "\n"

    return new_s


def is_valid_mode(p_mode):
    if p_mode in mode_list:
        return True

    if p_mode in mode_list_advanced:
        return True

    return False


def fatal_sql_error(err, sql, func):
    print("################################################")
    print("# FATAL SQL Error in " + str(func))
    print("#    SQL Message =  " + str(err.args[0]))
    print("#  SQL Statement = " + sql)
    print("################################################")
    exit_cleanly(1)


def exit_cleanly(p_rc):
    try:
        connL.close()
    except Exception:
        pass
    sys.exit(p_rc)


def cli_lock():
    try:
        fd = os.open(pid_file, os.O_RDONLY)
        ret = os.read(fd, 12)
        pid = ret.decode()
        os.close(fd)
    except IOError:
        return False
    except OSError:
        return False

    if not pid:
        return False

    try:
        os.kill(int(pid), 0)
    except OSError:
        return False

    return False


####################################################################
########                    MAINLINE                      ##########
####################################################################

## Initialize Globals ##############################################
REPO = util.get_value("GLOBAL", "REPO")

os.chdir(util.MY_HOME)

db_local = util.MY_LITE

connL = sqlite3.connect(db_local)

args = sys.argv

## process multiple commands seperated by ' : ' ###############
cmd = ""
is_colon = False
x = 1
while x < len(args):
    if args[x] == ":":
        is_colon = True
        util.cmd_system(cmd)
        cmd = ""
    else:
        cmd = str(cmd) + " " + str(args[x])

    x = x + 1

if is_colon:
    util.cmd_system(cmd)
    exit_cleanly(0)

## eliminate empty parameters ################################
while True:
    try:
        args.remove("")
    except Exception:
        break
full_cmd_line = " ".join(args[1:])

## validate inputs ###########################################
if len(args) == 1:
    api.info(False, MY_HOME, REPO)
    exit_cleanly(0)

if (args[1] == "--version") or (args[1] == "-v"):
    print("v" + util.get_version())
    exit_cleanly(0)


p_mode = ""
p_comp = "all"
installed_comp_list = meta.get_component_list()
available_comp_list = meta.get_available_component_list()
download_count = 0

not_fire = False;
if str(args[1]) not in (fire_list + fire_contrib):
    not_fire = True;

if (args[1] == "help") or (args[1] == "--help"):
    print(get_help_text())
    exit_cleanly(0)

## process global parameters #################

os.environ["isPreload"] = "True"
if "--no-preload" in args:
    args.remove("--no-preload")
    os.environ["isPreload"] = "False"
    os.environ["isRestart"] = "False"
else:
    os.environ["isRestart"] = "True"

if "--no-restart" in args:
    args.remove("--no-restart")
    os.environ["isRestart"] = "False"

if os.getenv("isJson") == "True":
    isJSON = True
else:
    isJSON = False
if "--json" in args:
    isJSON = True
    args.remove("--json")
    os.environ["isJson"] = "True"

isPRETTY = False
if "--jsonp" in args:
    isJSON = True
    isPRETTY = True
    args.remove("--jsonp")
    os.environ["isPretty"] = "True"

isDEBUG = 0
if "--debug" in args:
    args.remove("--debug")
    my_logger.info("Enabling DEBUG mode")
    logging.getLogger("cli_logger").setLevel(logging.DEBUG)
    my_logger.debug("DEBUG enabled")
    os.environ["pgeDebug"] = "1"

if "--debug2" in args:
    args.remove("--debug2")
    my_logger.info("Enabling DEBUG2 mode")
    logging.getLogger("cli_logger").setLevel(clilog.DEBUG2)
    my_logger.debug("DEBUG enabled")
    my_logger.debug2("DEBUG2 enabled")
    os.environ["pgeDebug"] = "2"

p_host = ""
p_home = ""
p_user = ""
p_passwd = ""
p_host_name = ""

isVERBOSE = False
if "--verbose" in args:
    isVERBOSE = True
    args.remove("--verbose")
if "-v" in args:
    isVERBOSE = True
    args.remove("-v")
if isVERBOSE:
    os.environ["isVerbose"] = "True"

isYES = False
if "-y" in args and not_fire:
    isYES = True
    args.remove("-y")
    os.environ["isYes"] = "True"

if "--pause" in args and not_fire:
    pause = str(get_next_arg("--pause"))
    if pause.isnumeric():
        os.environ["pgePause"] = str(pause)
        args.remove("--pause")
        args.remove(pause)
    else:
        util.exit_message(f"--pause parm {pause} must be numeric", 1)

if "--pg" in args:
    pgn = str(get_next_arg("--pg"))
    if pgn >= "14" and pgn <= "17":
        util.setenv("pgeN", pgn)
        args.remove("--pg")
        args.remove(pgn)
    else:
        util.exit_message(f"invalid --pg parm {pgn}", 1)

if "-U" in args:
    usr = get_next_arg("-U")
    if usr > "":
        args.remove("-U")
        args.remove(usr)
        util.setenv("pgeUser", usr)

if "-P" in args:
    passwd = get_next_arg("-P")
    if passwd > "":
        args.remove("-P")
        args.remove(passwd)
        util.setenv("pgePasswd", passwd)

if "-p" in args:
    port = get_next_arg("-p")
    if port > "":
        args.remove("-p")
        args.remove(port)
        util.setenv("pgePort", port)

isTIME = False
if "-t" in args and not_fire:
    isTIME = True
    args.remove("-t")
    os.environ["isTime"] = "True"

PGNAME = ""
i = 0
while i < len(args):
    arg = args[i]
    if arg == "-d" and not_fire:
        if i < (len(args) - 1):
            PGNAME = args[i + 1]
            os.environ["pgName"] = PGNAME
            args.remove(PGNAME)
            args.remove("-d")
            break
    i += 1

if "--test" in args and not_fire:
    util.isTEST = True
    os.environ["isTest"] = "True"
    args.remove("--test")

isSTART = False
if "--start" in args and not_fire:
    isSTART = True
    os.environ["isSTART"] = "True"
    args.remove("--start")

isDISABLED = False
if "--disabled" in args and not_fire:
    isDISABLED = True
    os.environ["isDISABLED"] = "True"
    args.remove("--disabled")

if util.get_stage() == "test":
    util.isTEST = True

if "--old" in args and not_fire:
    util.isSHOWDUPS = True
    args.remove("--old")
if "--show-duplicates" in args and not_fire:
    util.isSHOWDUPS = True
    args.remove("--show-duplicates")

isSVCS = False
if "--svcs" in args and "list" in args and not_fire:
    isSVCS = True
    os.environ["isSVCS"] = "True"
    args.remove("--svcs")

isFIPS = False
if "--fips" in args and "install" in args and not_fire:
    isFIPS = True
    os.environ["isFIPS"] = "True"
    args.remove("--fips")

isAUTOSTART = False
if "--autostart" in args and "install" in args and not_fire:
    isAUTOSTART = True
    util.setenv("isAutoStart", "True")
    args.remove("--autostart")

if "--rm-data" in args and not_fire:
    util.setenv("isRM_DATA", "True")
    args.remove("--rm-data")

isSILENT = False
if "--silent" in args and not_fire:
    isSILENT = True
    util.setenv("isSilent", "True")
    args.remove("--silent")

if len(args) == 1:
    util.exit_message("Nothing to do", 1, isJSON)

arg = 1
p_mode = args[1]

if (p_mode in no_log_commands) and (isJSON == True):
    pass
elif (
    p_mode in ("service", "spock", "um", "cluster")
    and (len(args) > 2)
    and (args[2] in no_log_commands)
):
    pass
else:
    my_logger.command(MY_CMD + " %s", util.scrub_passwd(full_cmd_line))

if not is_valid_mode(p_mode):
    util.exit_message("Invalid option or command '" + p_mode + "'", 1, isJSON)

if p_mode in lock_commands:
    if cli_lock():
        msg = (
            "Unable to execute '{0}', another instance may be running. \n"
            "HINT: Delete the lock file: '{1}' if no other instance is running.".format(
                p_mode, pid_file
            )
        )
        if isJSON:
            msg = '[{"state":"locked","msg":"' + msg + '"}]'
        util.exit_message(msg, 0)
    pid_fd = open(pid_file, "w")
    pid_fd.write(str(os.getpid()))
    pid_fd.close()

info_arg, p_comp_list, p_comp, p_version, extra_args = \
    util.get_comp_lists(p_mode, arg, args, ignore_comp_list, p_host, connL)

## PG_ISREADY #################################################################
if p_mode == "pg_isready":
    pg_v, spock_v = util.get_pg_v()
    cmd = "./pgedge pgbin " + pg_v.replace("pg", "") + " pg_isready"
    rc = os.system(cmd)
    if rc == 0:
        sys.exit(0)
    else:
        sys.exit(1)

## PSQL #######################################################################
psql_bad_msg = 'Two args required, try: psql "sql command" database'
if p_mode == "psql":
    if len(args) == 5:
        c_or_f = str(args[2])
        sql_cmd = str(args[3])
        db = str(args[4])
    elif len(args) == 4:
        c_or_f = "-c"
        sql_cmd = str(args[2])
        db = str(args[3])
    else:
        util.exit_message(psql_bad_msg)

    if sql_cmd == "-i":
        ## leave us at the interactive psql prompt
        sql_cmd = ""
    elif c_or_f in ("-f", "-c"):
        sql_cmd = f'{c_or_f} "' + sql_cmd + '" '
    else:
        util.exit_message(psql_bad_msg)

    pg_v, spock_v = util.get_pg_v()
    cmd = "./pgedge pgbin " + pg_v.replace("pg", "") + " 'psql " + sql_cmd + db + "'"

    if isVERBOSE:
        print(f"pg_v={pg_v}, sql_cmd={sql_cmd}, db={db}")
        print(f"cmd={cmd}")

    rc = os.system(cmd)
    if rc == 0:
        sys.exit(0)
    else:
        sys.exit(1)

## PGBIN #######################################################################
if p_mode == "pgbin":
    if len(args) != 4:
        util.exit_message(
            'invalid command, try: pgbin 15 "any valid command in pg15/bin"',
            1,
            isJSON,
        )

    pg_v = str(args[2])
    cmd_full = str(args[3])
    cmd_a = str(args[3]).split()
    cmd0 = cmd_a[0]

    if not pg_v.isnumeric():
        util.exit_message(
            "'" + pg_v + "' must be a numeric value for an installed pg version",
            1,
            isJSON,
        )

    pg_dir = "pg" + pg_v
    pg_bin = pg_dir + "/bin/"
    if not os.path.isdir(pg_dir):
        util.exit_message(f"postgres directory '{pg_dir}' not found", 1)

    cmd1 = pg_bin + cmd0
    if not os.path.exists(cmd1):
        util.exit_message("'" + cmd1 + "' not a valid pgbin command", 1, isJSON)

    cmd_parms = util.remove_suffix(";", cmd_full)
    cmd_parms = util.remove_prefix(cmd0, cmd_parms)
    cmd_parms_arr = cmd_parms.split(";")
    if len(cmd_parms_arr) > 1:
        util.exit_message(
            "command params must not contain an embedded semi-colon", 1, isJSON
        )

    port = util.get_comp_port(pg_dir)
    final_safe_cmd = cmd1 + " " + cmd_parms + " --port=" + str(port)
    if isVERBOSE:
        print(final_safe_cmd)

    rc = os.system(final_safe_cmd)
    if rc == 0:
        sys.exit(0)

    sys.exit(1)

if p_mode in py3_check_list:
    util.py3_check()

## FIRE LIST ###############################################################
if (p_mode in fire_list) or (p_mode in fire_contrib):
    fire_away(p_mode, args)

## NATIVE_LIST #######################################
if p_mode in native_list:
    cmd = ""
    for n in range(2, len(args)):
        cmd = cmd + " " + args[n]

    bin_path = ""
    if p_mode == "bouncer":
       bin_path = os.path.join(MY_HOME, "bouncer", "bin", "pgbouncer")
    elif p_mode == "backrest":
       bin_path = os.path.join(MY_HOME, "backrest", "backrest.py")
    elif p_mode == "etcd":
       bin_path = os.path.join(MY_HOME, "etcd", "etcd.py")
    elif p_mode == "patroni":
       bin_path = os.path.join(MY_HOME, "patroni", "ha_patroni.py")
    elif p_mode == "ansible":
       bin_path = "/usr/local/bin/ansible"

    util.run_native(p_mode, bin_path, cmd)

    exit_cleanly(0)

## TOP #####################################################################
if p_mode == "top":
    try:
        api.top(display=False)
        if isJSON:
            time.sleep(0.5)
            api.top(display=True, isJson=isJSON)
            exit_cleanly(0)
        while True:
            api.top(display=True)
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    exit_cleanly(0)

## INFO ####################################################################
if p_mode == "info":
    if p_comp == "all" and info_arg == 0:
        api.info(isJSON, MY_HOME, REPO)
    else:
        try:
            c = connL.cursor()
            datadir = ""
            logdir = ""
            svcname = ""
            svcuser = ""
            port = 0
            is_installed = 0
            autostart = ""
            version = ""
            install_dt = ""
            if p_comp != "all":
                sql = (
                    "SELECT coalesce(c.datadir,''), coalesce(c.logdir,''), \n"
                    "       coalesce(c.svcname,''), coalesce(c.svcuser,''), \n"
                    "       c.port, c.autostart, c.version, c.install_dt, \n"
                    + "       coalesce((select release_date from versions where c.component = component and c.version = version),'20160101') "
                    + "  FROM components c WHERE c.component = '"
                    + p_comp
                    + "'"
                )
                c.execute(sql)
                data = c.fetchone()
                if not data is None:
                    is_installed = 1
                    datadir = str(data[0])
                    logdir = str(data[1])
                    svcname = str(data[2])
                    svcuser = str(data[3])
                    port = int(data[4])
                    autostart = str(data[5])
                    version = str(data[6])
                    install_dt = str(data[7])
                    ins_release_dt = str(data[8])

            sql = (
                "SELECT c.category, c.description, p.project, r.component, v.version, \n"
                + "       v.platform, p.sources_url, p.project_url, v.is_current, \n"
                + "       "
                + str(is_installed)
                + " as is_installed, r.stage, \n"
                + "       '', v.release_date, p.is_extension, \n"
                + "       r.disp_name, v.pre_reqs, r.license, p.description, \n"
                + "       r.is_available, r.available_ver \n"
                + "  FROM projects p, releases r, versions v, categories c \n"
                + " WHERE r.project = p.project AND v.component = r.component \n"
                + "   AND "
                + util.like_pf("v.platform")
                + " \n"
                + "   AND p.category = c.category"
            )

            if p_comp != "all":
                sql = sql + " AND r.component = '" + p_comp + "'"

            if p_version != "" and p_version != "all":
                sql = sql + " and v.version = '" + p_version + "'"

            sql = (
                sql
                + "\n ORDER BY v.is_current desc,is_installed, c.category, p.project, r.component desc, v.version desc "
            )

            if p_version == "":
                sql = sql + " limit 1"

            c.execute(sql)
            data = c.fetchall()

            compJson = []
            kount = 0
            for row in data:
                kount = kount + 1
                cat = row[0]
                cat_desc = row[1]
                proj = row[2]
                comp = row[3]
                ver = row[4]
                plat = row[5]
                sources_url = row[6]
                project_url = row[7]
                is_current = row[8]
                is_installed = row[9]
                stage = row[10]
                sup_plat = row[11]
                rel_dt = str(row[12])
                is_extension = row[13]
                disp_name = row[14]
                pre_reqs = row[15]
                license = row[16]
                proj_description = row[17]
                is_available = row[18]
                available_ver = row[19]
                if len(rel_dt) == 8:
                    release_date = rel_dt[:4] + "-" + rel_dt[4:6] + "-" + rel_dt[6:]
                else:
                    release_date = rel_dt
                if len(install_dt) >= 8:
                    install_date = (
                        install_dt[0:4]
                        + "-"
                        + install_dt[5:7]
                        + "-"
                        + install_dt[8:10]
                    )
                else:
                    install_date = install_dt
                compDict = {}
                compDict["is_available"] = is_available
                compDict["available_ver"] = available_ver
                compDict["category"] = cat
                compDict["project"] = proj
                compDict["component"] = comp
                compDict["platform"] = plat
                compDict["sources_url"] = sources_url
                compDict["proj_description"] = proj_description
                compDict["project_url"] = project_url
                compDict["is_installed"] = is_installed
                compDict["is_extension"] = is_extension
                compDict["disp_name"] = disp_name
                compDict["pre_reqs"] = pre_reqs
                compDict["license"] = license
                current_version = meta.get_current_version(comp)
                compDict["version"] = ver
                if is_installed == 1:
                    compDict["ins_release_dt"] = (
                        ins_release_dt[:4]
                        + "-"
                        + ins_release_dt[4:6]
                        + "-"
                        + ins_release_dt[6:]
                    )
                    compDict["version"] = version
                    is_update_available = 0
                    cv = Version.coerce(current_version)
                    iv = Version.coerce(version)
                    if cv > iv:
                        is_update_available = 1
                    if current_version == version:
                        is_current = 1
                    elif is_update_available == 1:
                        compDict["current_version"] = current_version
                compDict["is_current"] = is_current
                compDict["stage"] = stage
                compDict["sup_plat"] = sup_plat
                compDict["release_date"] = release_date

                compDict["is_new"] = 0

                try:
                    rd = datetime.datetime.strptime(release_date, "%Y-%m-%d")
                    today_date = datetime.datetime.today()
                    date_diff = (today_date - rd).days

                    if date_diff <= 30:
                        compDict["is_new"] = 1
                except Exception:
                    pass

                compDict["install_date"] = install_date
                compDict["datadir"] = datadir
                compDict["logdir"] = logdir
                compDict["svcname"] = svcname
                compDict["svcuser"] = svcuser
                compDict["port"] = port
                compDict["autostart"] = autostart
                if is_installed == 1 and port > 0:
                    is_running = util.check_comp(comp, port, 0, True)
                    if is_running == "NotInitialized":
                        compDict["available_port"] = util.get_avail_port(
                            "PG Port", port, comp, isJSON=True
                        )
                        compDict["available_datadir"] = os.path.join(
                            MY_HOME, "data", comp
                        )
                        compDict["port"] = 0
                    compDict["status"] = is_running
                    compDict["current_logfile"] = ""

                compJson.append(compDict)

                if not isJSON:
                    api.info_component(compDict, kount)
            if isJSON:
                print(json.dumps(compJson, sort_keys=True, indent=2))
        except Exception as e:
            fatal_sql_error(e, sql, "INFO")
    exit_cleanly(0)

## STATUS ####################################################
if p_mode == "status":
    args.insert(0,p_mode)
    fire_away("service", args)

## DOWNLOAD, CLEAN, LIST, REMOVE ############################
if p_mode in ["download", "clean", "list", "remove"]:
    args.insert(0, p_mode)
    fire_away("um", args)

## INSTALL ################################################
if p_mode == "install":
    if p_comp == "all":
        msg = "You must specify component to install."
        my_logger.error(msg)
        return_code = 1
        if isJSON:
            return_code = 0
            msg = '[{"status":"error","msg":"' + msg + '"}]'
        util.exit_message(msg, return_code)

    if meta.get_stage(p_comp) == "included":
        util.exit_message(
            "this component is already included in our postgres binaries", 0
        )

    if not isSILENT:
        util.message("\n########### Installing " + p_comp + " ###############")

    deplist = get_depend_list(p_comp_list)
    component_installed = False
    dependent_components = []
    installed_commponents = []
    dependencies = [p for p in deplist if p not in p_comp_list]
    for c in deplist:
        if p_version and c in p_comp_list:
            status = install_comp(c, p_version)
        else:
            p_version = None
            status = install_comp(c)

        if status == "enable":
            p_mode = "enable"

        p_version = util.trim_plat(p_version)
        update_component_state(c, p_mode, p_version)
        isExt = meta.is_extension(c)
        if isExt:
            parent = util.get_parent_component(c, 0)
        if status == 1 and (c in p_comp_list or p_comp_list[0] == "all"):
            if isExt:
                ## just run the CREATE EXTENSION sql command without reboot or change preloads
                util.create_extension(parent, c, False, enable=False)
            else:
                ## already installed
                pass
        elif status != 1:
            installed_comp_list.append(c)
            isExt = meta.is_extension(c)
            if isExt:
                util.copy_extension_files(c, parent)
            script_name = "install-" + c
            util.run_script(c, script_name, meta.get_current_version(c))
            if isJSON:
                json_dict = {}
                json_dict["state"] = "install"
                json_dict["status"] = "complete"
                json_dict["msg"] = "Successfully installed the component."
                json_dict["component"] = c
                msg = json.dumps([json_dict])
                print(msg)
            if c in p_comp_list or p_comp_list[0] == "all":
                component_installed = True
                installed_commponents.append(c)
                if isExt:
                    util.delete_dir(os.path.join(MY_HOME, c))
            else:
                dependent_components.append(c)

    exit_cleanly(0)

# Verify data & log directories ############################
data_home = MY_HOME + os.sep + "data"
if not os.path.exists(data_home):
    os.mkdir(data_home)
    data_logs = data_home + os.sep + "logs"
    os.mkdir(data_logs)

script_name = ""

## UPDATE ###################################################
if p_mode == "update":
    util.retrieve_remote()

    if not isJSON:
        print(" ")

    try:
        l = connL.cursor()
        rel_date = (
            "substr(cast(release_date as text), 1, 4) "
            + " || '-' || substr(cast(release_date as text), 5, 2) "
            + " ||  '-' || substr(cast(release_date as text), 7, 2)"
        )
        days_since_release = "julianday('now') - julianday(" + rel_date + ")"
        sql = (
            " SELECT v.component, v.version, v.platform, 'not installed' as status, \n"
            + "        "
            + rel_date
            + " as rel_date, r.stage, c.description, c.category\n"
            + "   FROM versions v, releases r, projects p, categories c \n"
            + "  WHERE v.component = r.component AND r.project = p.project \n"
            + "    AND p.category = c.category \n"
            + "    AND is_current = 1 \n"
            + "    AND "
            + days_since_release
            + " <= 31 \n"
            + "    AND "
            + util.like_pf("platform")
            + " \n"
            + "    AND v.component NOT IN (SELECT component FROM components) \n"
            + "UNION ALL \n"
            + " SELECT v.component, v.version, v.platform, 'installed', \n"
            + "        "
            + rel_date
            + " as rel_date, r.stage, c.description, c.category \n"
            + "   FROM versions v, releases r, projects p, categories c \n"
            + "  WHERE v.component = r.component  AND r.project = p.project \n"
            + "    AND p.category = c.category \n"
            + "    AND is_current = 1 \n"
            + "    AND "
            + days_since_release
            + " <= 31 \n"
            + "    AND "
            + util.like_pf("platform")
            + " \n"
            + "    AND v.component IN (SELECT component FROM components) \n"
            + "ORDER BY 4, 8, 1, 2"
        )

        l.execute(sql)
        rows = l.fetchall()
        l.close()

        hasUpdates = 0
        kount = 0
        jsonList = []

        for row in rows:
            compDict = {}

            compDict["category"] = str(row[6])
            compDict["component"] = str(row[0])
            compDict["version"] = str(row[1])
            compDict["rel_date"] = str(row[4])
            compDict["status"] = str(row[3])

            stage = str(row[5])
            if stage == "test":
                if not util.isTEST:
                    continue

            if stage in ("bring-own", "included", "soon"):
                continue

            hasUpdates = 1
            kount = kount + 1
            jsonList.append(compDict)

        if not isJSON and not isSILENT:
            if kount >= 1:
                print(
                    "--- New components & extensions released in the last 30 days ---"
                )
                headers = [
                    "Category",
                    "Component",
                    "Version",
                    "ReleaseDt",
                    "Status",
                ]
                keys = ["category", "component", "version", "rel_date", "status"]
                print(api.format_data_to_table(jsonList, keys, headers))
            else:
                print("--- No new components released in last 30 days ---")
            print(" ")

        [last_update_utc, last_update_local, unique_id] = util.read_hosts(
            "localhost"
        )
        util.update_hosts("localhost", unique_id, True)

        if isJSON:
            print('[{"status":"completed","has_updates":' + str(hasUpdates) + "}]")
        else:
            if not isSILENT:
                print(
                    "---------- Components available to install or update ------------"
                )
                meta.get_list(isJSON, p_comp=p_comp)
    except Exception as e:
        fatal_sql_error(e, sql, "UPDATE in mainline")

## ENABLE, DISABLE ###########################################
if p_mode == "enable" or p_mode == "disable":
    args.insert(0, p_mode)
    fire_away("service", args)

## CONFIG, INIT, RELOAD ##################################
if p_mode in ["config", "init", "reload"]:
    util.check_server(p_comp, p_mode)
    sys.exit(util.run_script(p_comp, f"{p_mode}-{p_comp}", extra_args))

## STOP component(s) #####################################
if (p_mode == "stop") or (p_mode == "kill"):
    args.insert(0,p_mode)
    fire_away("service", args)

## START #################################################
if (p_mode == "start"):
    args.insert(0,p_mode)
    fire_away("service", args)

## RESTART ###############################################
if (p_mode == "restart"):
    args.insert(0,p_mode)
    fire_away("service", args)

## UPGRADE ##################################################
if p_mode == "upgrade":
    os.environ["isRestart"] = "False"
    if p_comp == "all":
        updates_comp = []
        comp_list = meta.get_list(False, p_return=True)
        for c in comp_list:
            if c.get("updates") == 1:
                updates_comp.append(c)
        updates_cnt = len(updates_comp)
        if not isJSON and updates_cnt > 0:
            headers = [
                "Category",
                "Component",
                "Version",
                "ReleaseDt",
                "Status",
                "Updates",
            ]
            keys = [
                "category_desc",
                "component",
                "version",
                "release_date",
                "status",
                "current_version",
            ]
            print(api.format_data_to_table(updates_comp, keys, headers))
            isYES = False
        upgrade_flag = 1
        if isYES:
            for comp in updates_comp:
                rc = upgrade_component(comp.get("component"))
                if rc == 0:
                    upgrade_flag = 0
        if upgrade_flag == 1 and updates_cnt == 0:
            msg = "All components are already upgraded to the latest version."
            print(msg)
            my_logger.info(msg)
    else:
        rc = upgrade_component(p_comp)

        if rc == 1:
            msg = "Nothing to upgrade."
            print(msg)
            my_logger.info(msg)

## VERIFY #############################################
if p_mode == "verify":
    util.verify(isJSON)
    exit_cleanly(0)

## SET #################################################
if p_mode == "set":
    if len(args) == 5:
        if args[2] == "con":
            util.set_con(args[3], args[4])
        else:
            util.set_value(args[2], args[3], args[4])
    else:
        print("ERROR: The SET command must have 3 parameters.")
        exit_cleanly(1)

    exit_cleanly(0)

## GET ################################################
if p_mode == "get":
    if len(args) == 4:
        if args[2] == "con":
            util.get_con(args[3])
        else:
            print(util.get_value(args[2], args[3]))
    else:
        print("ERROR: The GET command must have 2 parameters.")
        exit_cleanly(1)

    exit_cleanly(0)

## UNSET ##############################################
if p_mode == "unset":
    if len(args) == 4:
        util.unset_value(args[2], args[3])
    else:
        print("ERROR: The UNSET command must have 2 parameters.")
        exit_cleanly(1)
    exit_cleanly(0)

## CHANGE-PGCONF #####################################
if p_mode == "change-pgconf":
    if (len(args)) < 5 or (len(args) > 6):
        print(
            "ERROR: The CHANGE-PGCONF command must have 3 or 4 parameters: pgver key value [isReplace=True]."
        )
        exit_cleanly(1)

    isReplace = True
    if len(args) == 6:
        if (args[5] == "False") or (args[5] == "false"):
            isReplace = False

    pgV = args[2]
    if meta.is_present("components", "component", pgV, "one"):
        pass
    else:
        print("ERROR: " + pgV + " is not installed")
        exit_cleanly(1)

    exit_cleanly(util.change_pgconf_keyval(pgV, args[3], args[4], isReplace))

## USERADD ############################################
if p_mode == "useradd":
    if len(args) == 3:
        exit_cleanly(startup.useradd_linux(args[2]))
    else:
        print("ERROR: The USERADD command must have 1 parameter (svcuser).")
        exit_cleanly(1)

## TUNE ###############################################
if p_mode == "tune":
    if len(args) == 3:
        exit_cleanly(util.tune_postgresql_conf(args[2]))
    else:
        print("ERROR: The TUNE command must have 1 parameter (pgver).")
        exit_cleanly(1)

util.exit_cleanly(0,connL)