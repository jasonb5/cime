"""
Post run I/O processing
case_post_run_io is a member of class Case from file case.py
"""
import os
from CIME.XML.standard_module_setup import *
from CIME.utils                     import run_and_log_case_status

logger = logging.getLogger(__name__)

###############################################################################
def _convert_adios_to_nc(case):
###############################################################################
    """
    Converts all ADIOS output files for the case to NetCDF format
    """
    env_mach_specific = case.get_env('mach_specific')
  
    # Create the ADIOS convertion tool command,
    # "<EXEROOT>/adios2pio-nm.exe --idir=<RUNDIR>"
    # The command converts all ADIOS BP files in <RUNDIR> to
    # NetCDF files

    # The ADIOS conversion tool is installed in EXEROOT
    exeroot = case.get_value("EXEROOT")
    adios_conv_tool_name = "adios2pio-nm.exe"
    adios_conv_tool_exe = os.path.join(exeroot, adios_conv_tool_name)

    # The ADIOS output files should be in RUNDIR
    rundir = case.get_value("RUNDIR")

    adios_conv_tool_args = "--idir=" + rundir
    adios_conv_tool_cmd = adios_conv_tool_exe + " " + adios_conv_tool_args

    # Replace logfile name, "e3sm.log.*" with "e3sm_adios_post_io.log.*"
    # The logfile name is part of the run command suffix
    adios_conv_tool_cmd_suffix = env_mach_specific.get_value("run_misc_suffix")
    adios_conv_tool_cmd_suffix = adios_conv_tool_cmd_suffix.replace(
                                  "e3sm.log", "e3sm_adios_post_io.log")

    # Get the current mpirun command (for e3sm.exe)
    cmd = case.get_mpirun_cmd(allow_unresolved_envvars=False)

    # Create mpirun command for the ADIOS conversion tool
    # Replace run_exe and run_misc_suffix in mpirun command
    # with ADIOS convertion tool command and suffix
    run_exe = env_mach_specific.get_value("run_exe", resolved=True)
    run_exe = case.get_resolved_value(run_exe);
    run_misc_suffix = env_mach_specific.get_value("run_misc_suffix")
    cmd = cmd.replace(run_exe, adios_conv_tool_cmd)
    cmd = cmd.replace(run_misc_suffix, adios_conv_tool_cmd_suffix)
    logger.info("Run command for ADIOS post processing is : {}".format(cmd))

    # Load the environment
    case.load_env(reset=True)

    run_func = lambda: run_cmd(cmd, from_dir=rundir)[0]

    # Run the modified case
    success = run_and_log_case_status(run_func,
                "ADIOS to NetCDF conversion",
                caseroot=case.get_value("CASEROOT"))

    return success

###############################################################################
def case_post_run_io(self):
###############################################################################
    """
    I/O Post processing :
    1. Convert ADIOS output files, if any, to NetCDF
    """
    success = True
    has_adios = False
    self.load_env(job="case.post_run_io")
    component_classes = [ "ATM", "CPL", "OCN", "WAV", "GLC", "ICE",
                          "ROF", "LND", "ESP" ]
    # Check if user chose "adios" as the iotype for any component
    for compclass in component_classes:
        key = "PIO_TYPENAME_{}".format(compclass)
        pio_typename = self.get_value(key)
        if pio_typename == "adios":
            has_adios = True
            break
    if has_adios:
        logger.info("I/O post processing for ADIOS starting")
        success = _convert_adios_to_nc(self)
        logger.info("I/O post processing for ADIOS completed")
    else:
        logger.info("No I/O post processing required")

    return success