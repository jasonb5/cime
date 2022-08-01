#!/usr/bin/env python3

import tempfile
import unittest
from unittest import mock

from CIME.XML.env_batch import EnvBatch

# pylint: disable=unused-argument


class TestXMLEnvBatch(unittest.TestCase):
    def test_get_submit_args(self):
        with tempfile.NamedTemporaryFile() as tf:
            tf.write(
                b"""<?xml version="1.0"?>
<file id="env_batch.xml" version="2.0">
  <header>
      These variables may be changed anytime during a run, they
      control arguments to the batch submit command.
    </header>
  <group id="config_batch">
    <entry id="BATCH_SYSTEM" value="none">
      <type>char</type>
      <valid_values>miller_slurm,nersc_slurm,lc_slurm,moab,pbs,lsf,slurm,cobalt,cobalt_theta,none</valid_values>
      <desc>The batch system type to use for this machine.</desc>
    </entry>
  </group>
  <group id="job_submission">
    <entry id="PROJECT_REQUIRED" value="FALSE">
      <type>logical</type>
      <valid_values>TRUE,FALSE</valid_values>
      <desc>whether the PROJECT value is required on this machine</desc>
    </entry>
  </group>
  <batch_system type="none">
    <batch_query args=""/>
    <batch_submit/>
    <batch_cancel/>
    <batch_redirect/>
    <batch_directive/>
    <submit_args>
      <arg flag="--time" name="$JOB_WALLCLOCK_TIME"/>
      <arg flag="-p" name="$JOB_QUEUE"/>
      <arg flag="--account" name="$PROJECT"/>
      <arg flag="-n" name=" $TOTALPES/$MAX_MPITASKS_PER_NODE"/>
      <arg flag="--mode script"/>
    </submit_args>
    <directives>
      <directive/>
    </directives>
  </batch_system>
</file>"""
            )
            tf.seek(0)

            batch = EnvBatch(infile=tf.name, read_only=False)

            case = mock.MagicMock()
            case.get_resolved_value.return_value = "5"
            case.get_value.side_effect = [
                "01:00:00",
                "debug",
                "test_project",
            ]

            submit_args = batch.get_submit_args(case, "case.run")

        assert submit_args == "  --time 01:00:00 -p debug --account test_project -n 5 --mode script"

    @mock.patch("CIME.XML.env_batch.EnvBatch.get")
    def test_get_queue_specs(self, get):
        node = mock.MagicMock()

        batch = EnvBatch()

        get.side_effect = [
            "1",
            "1",
            None,
            None,
            "case.run",
            "08:00:00",
            "05:00:00",
            "12:00:00",
            "false",
        ]

        (
            nodemin,
            nodemax,
            jobname,
            walltimedef,
            walltimemin,
            walltimemax,
            jobmin,
            jobmax,
            strict,
        ) = batch.get_queue_specs(node)

        self.assertTrue(nodemin == 1)
        self.assertTrue(nodemax == 1)
        self.assertTrue(jobname == "case.run")
        self.assertTrue(walltimedef == "08:00:00")
        self.assertTrue(walltimemin == "05:00:00")
        self.assertTrue(walltimemax == "12:00:00")
        self.assertTrue(jobmin == None)
        self.assertTrue(jobmax == None)
        self.assertFalse(strict)

    @mock.patch("CIME.XML.env_batch.EnvBatch.text", return_value="default")
    # nodemin, nodemax, jobname, walltimemin, walltimemax, jobmin, jobmax, strict
    @mock.patch(
        "CIME.XML.env_batch.EnvBatch.get_queue_specs",
        return_value=[
            1,
            1,
            "case.run",
            "10:00:00",
            "08:00:00",
            "12:00:00",
            1,
            1,
            False,
        ],
    )
    @mock.patch("CIME.XML.env_batch.EnvBatch.select_best_queue")
    @mock.patch("CIME.XML.env_batch.EnvBatch.get_default_queue")
    def test_set_job_defaults_honor_walltimemax(
        self, get_default_queue, select_best_queue, get_queue_specs, text
    ):
        case = mock.MagicMock()

        batch_jobs = [
            (
                "case.run",
                {
                    "template": "template.case.run",
                    "prereq": "$BUILD_COMPLETE and not $TEST",
                },
            )
        ]

        def get_value(*args, **kwargs):
            if args[0] == "USER_REQUESTED_WALLTIME":
                return "20:00:00"

            return mock.MagicMock()

        case.get_value = get_value

        case.get_env.return_value.get_jobs.return_value = ["case.run"]

        batch = EnvBatch()

        batch.set_job_defaults(batch_jobs, case)

        env_workflow = case.get_env.return_value

        env_workflow.set_value.assert_any_call(
            "JOB_QUEUE", "default", subgroup="case.run", ignore_type=False
        )
        env_workflow.set_value.assert_any_call(
            "JOB_WALLCLOCK_TIME", "20:00:00", subgroup="case.run"
        )

    @mock.patch("CIME.XML.env_batch.EnvBatch.text", return_value="default")
    # nodemin, nodemax, jobname, walltimemin, walltimemax, jobmin, jobmax, strict
    @mock.patch(
        "CIME.XML.env_batch.EnvBatch.get_queue_specs",
        return_value=[
            1,
            1,
            "case.run",
            "10:00:00",
            "08:00:00",
            "12:00:00",
            1,
            1,
            False,
        ],
    )
    @mock.patch("CIME.XML.env_batch.EnvBatch.select_best_queue")
    @mock.patch("CIME.XML.env_batch.EnvBatch.get_default_queue")
    def test_set_job_defaults_honor_walltimemin(
        self, get_default_queue, select_best_queue, get_queue_specs, text
    ):
        case = mock.MagicMock()

        batch_jobs = [
            (
                "case.run",
                {
                    "template": "template.case.run",
                    "prereq": "$BUILD_COMPLETE and not $TEST",
                },
            )
        ]

        def get_value(*args, **kwargs):
            if args[0] == "USER_REQUESTED_WALLTIME":
                return "05:00:00"

            return mock.MagicMock()

        case.get_value = get_value

        case.get_env.return_value.get_jobs.return_value = ["case.run"]

        batch = EnvBatch()

        batch.set_job_defaults(batch_jobs, case)

        env_workflow = case.get_env.return_value

        env_workflow.set_value.assert_any_call(
            "JOB_QUEUE", "default", subgroup="case.run", ignore_type=False
        )
        env_workflow.set_value.assert_any_call(
            "JOB_WALLCLOCK_TIME", "05:00:00", subgroup="case.run"
        )

    @mock.patch("CIME.XML.env_batch.EnvBatch.text", return_value="default")
    # nodemin, nodemax, jobname, walltimemax, jobmin, jobmax, strict
    @mock.patch(
        "CIME.XML.env_batch.EnvBatch.get_queue_specs",
        return_value=[
            1,
            1,
            "case.run",
            "10:00:00",
            "08:00:00",
            "12:00:00",
            1,
            1,
            False,
        ],
    )
    @mock.patch("CIME.XML.env_batch.EnvBatch.select_best_queue")
    @mock.patch("CIME.XML.env_batch.EnvBatch.get_default_queue")
    def test_set_job_defaults_user_walltime(
        self, get_default_queue, select_best_queue, get_queue_specs, text
    ):
        case = mock.MagicMock()

        batch_jobs = [
            (
                "case.run",
                {
                    "template": "template.case.run",
                    "prereq": "$BUILD_COMPLETE and not $TEST",
                },
            )
        ]

        def get_value(*args, **kwargs):
            if args[0] == "USER_REQUESTED_WALLTIME":
                return "10:00:00"

            return mock.MagicMock()

        case.get_value = get_value

        case.get_env.return_value.get_jobs.return_value = ["case.run"]

        batch = EnvBatch()

        batch.set_job_defaults(batch_jobs, case)

        env_workflow = case.get_env.return_value

        env_workflow.set_value.assert_any_call(
            "JOB_QUEUE", "default", subgroup="case.run", ignore_type=False
        )
        env_workflow.set_value.assert_any_call(
            "JOB_WALLCLOCK_TIME", "10:00:00", subgroup="case.run"
        )

    @mock.patch("CIME.XML.env_batch.EnvBatch.text", return_value="default")
    # nodemin, nodemax, jobname, walltimemax, jobmin, jobmax, strict
    @mock.patch(
        "CIME.XML.env_batch.EnvBatch.get_queue_specs",
        return_value=[
            1,
            1,
            "case.run",
            "10:00:00",
            "05:00:00",
            None,
            1,
            1,
            False,
        ],
    )
    @mock.patch("CIME.XML.env_batch.EnvBatch.select_best_queue")
    @mock.patch("CIME.XML.env_batch.EnvBatch.get_default_queue")
    def test_set_job_defaults_walltimemax_none(
        self, get_default_queue, select_best_queue, get_queue_specs, text
    ):
        case = mock.MagicMock()

        batch_jobs = [
            (
                "case.run",
                {
                    "template": "template.case.run",
                    "prereq": "$BUILD_COMPLETE and not $TEST",
                },
            )
        ]

        def get_value(*args, **kwargs):
            if args[0] == "USER_REQUESTED_WALLTIME":
                return "08:00:00"

            return mock.MagicMock()

        case.get_value = get_value

        case.get_env.return_value.get_jobs.return_value = ["case.run"]

        batch = EnvBatch()

        batch.set_job_defaults(batch_jobs, case)

        env_workflow = case.get_env.return_value

        env_workflow.set_value.assert_any_call(
            "JOB_QUEUE", "default", subgroup="case.run", ignore_type=False
        )
        env_workflow.set_value.assert_any_call(
            "JOB_WALLCLOCK_TIME", "08:00:00", subgroup="case.run"
        )

    @mock.patch("CIME.XML.env_batch.EnvBatch.text", return_value="default")
    # nodemin, nodemax, jobname, walltimemax, jobmin, jobmax, strict
    @mock.patch(
        "CIME.XML.env_batch.EnvBatch.get_queue_specs",
        return_value=[
            1,
            1,
            "case.run",
            "10:00:00",
            None,
            "12:00:00",
            1,
            1,
            False,
        ],
    )
    @mock.patch("CIME.XML.env_batch.EnvBatch.select_best_queue")
    @mock.patch("CIME.XML.env_batch.EnvBatch.get_default_queue")
    def test_set_job_defaults_walltimemin_none(
        self, get_default_queue, select_best_queue, get_queue_specs, text
    ):
        case = mock.MagicMock()

        batch_jobs = [
            (
                "case.run",
                {
                    "template": "template.case.run",
                    "prereq": "$BUILD_COMPLETE and not $TEST",
                },
            )
        ]

        def get_value(*args, **kwargs):
            if args[0] == "USER_REQUESTED_WALLTIME":
                return "08:00:00"

            return mock.MagicMock()

        case.get_value = get_value

        case.get_env.return_value.get_jobs.return_value = ["case.run"]

        batch = EnvBatch()

        batch.set_job_defaults(batch_jobs, case)

        env_workflow = case.get_env.return_value

        env_workflow.set_value.assert_any_call(
            "JOB_QUEUE", "default", subgroup="case.run", ignore_type=False
        )
        env_workflow.set_value.assert_any_call(
            "JOB_WALLCLOCK_TIME", "08:00:00", subgroup="case.run"
        )

    @mock.patch("CIME.XML.env_batch.EnvBatch.text", return_value="default")
    # nodemin, nodemax, jobname, walltimemax, jobmin, jobmax, strict
    @mock.patch(
        "CIME.XML.env_batch.EnvBatch.get_queue_specs",
        return_value=[
            1,
            1,
            "case.run",
            "10:00:00",
            "08:00:00",
            "12:00:00",
            1,
            1,
            False,
        ],
    )
    @mock.patch("CIME.XML.env_batch.EnvBatch.select_best_queue")
    @mock.patch("CIME.XML.env_batch.EnvBatch.get_default_queue")
    def test_set_job_defaults_walltimedef(
        self, get_default_queue, select_best_queue, get_queue_specs, text
    ):
        case = mock.MagicMock()

        batch_jobs = [
            (
                "case.run",
                {
                    "template": "template.case.run",
                    "prereq": "$BUILD_COMPLETE and not $TEST",
                },
            )
        ]

        def get_value(*args, **kwargs):
            if args[0] == "USER_REQUESTED_WALLTIME":
                return None

            return mock.MagicMock()

        case.get_value = get_value

        case.get_env.return_value.get_jobs.return_value = ["case.run"]

        batch = EnvBatch()

        batch.set_job_defaults(batch_jobs, case)

        env_workflow = case.get_env.return_value

        env_workflow.set_value.assert_any_call(
            "JOB_QUEUE", "default", subgroup="case.run", ignore_type=False
        )
        env_workflow.set_value.assert_any_call(
            "JOB_WALLCLOCK_TIME", "10:00:00", subgroup="case.run"
        )

    @mock.patch("CIME.XML.env_batch.EnvBatch.text", return_value="default")
    # nodemin, nodemax, jobname, walltimemax, jobmin, jobmax, strict
    @mock.patch(
        "CIME.XML.env_batch.EnvBatch.get_queue_specs",
        return_value=[
            1,
            1,
            "case.run",
            None,
            "08:00:00",
            "12:00:00",
            1,
            1,
            False,
        ],
    )
    @mock.patch("CIME.XML.env_batch.EnvBatch.select_best_queue")
    @mock.patch("CIME.XML.env_batch.EnvBatch.get_default_queue")
    def test_set_job_defaults(
        self, get_default_queue, select_best_queue, get_queue_specs, text
    ):
        case = mock.MagicMock()

        batch_jobs = [
            (
                "case.run",
                {
                    "template": "template.case.run",
                    "prereq": "$BUILD_COMPLETE and not $TEST",
                },
            )
        ]

        def get_value(*args, **kwargs):
            if args[0] == "USER_REQUESTED_WALLTIME":
                return None

            return mock.MagicMock()

        case.get_value = get_value

        case.get_env.return_value.get_jobs.return_value = ["case.run"]

        batch = EnvBatch()

        batch.set_job_defaults(batch_jobs, case)

        env_workflow = case.get_env.return_value

        env_workflow.set_value.assert_any_call(
            "JOB_QUEUE", "default", subgroup="case.run", ignore_type=False
        )
        env_workflow.set_value.assert_any_call(
            "JOB_WALLCLOCK_TIME", "12:00:00", subgroup="case.run"
        )


if __name__ == "__main__":
    unittest.main()
